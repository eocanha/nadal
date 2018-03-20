#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>
#include <memory>
#include <cassert>
#include <unistd.h>
#include <thread>
#include <vector>
#include <istream>
#include <fstream>
#include <iterator>
#include <condition_variable>
#include <mutex>
#include <iostream>
#include <algorithm>
#include <functional>
#include <string.h>
using namespace std;

GST_DEBUG_CATEGORY_STATIC(testcase_debug);
#define GST_CAT_DEFAULT testcase_debug

class Barrier {
    int m_counter;
    condition_variable m_condition;
    mutex m_mutex;

public:
    void notify() {
        lock_guard<mutex> lockGuard(m_mutex);
        assert(m_counter == 0);
        m_counter++;
        m_condition.notify_all();
    }

    void waitAndConsume() {
        unique_lock<mutex> uniqueLock(m_mutex);
        m_condition.wait(uniqueLock, [this] { return m_counter > 0; });

        assert(m_counter == 1);
        m_counter--;
    }
};

static void
print_error_message (GstMessage * msg)
{
  GError *err = NULL;
  gchar *name, *debug = NULL;

  name = gst_object_get_path_string (msg->src);
  gst_message_parse_error (msg, &err, &debug);

  g_printerr ("ERROR: from element %s: %s\n", name, err->message);
  if (debug != NULL)
    g_printerr ("Additional debug info:\n%s\n", debug);

  g_clear_error (&err);
  g_free (debug);
  g_free (name);
}

static GstBusSyncReply
bus_sync_handler (GstBus * bus, GstMessage * message, gpointer data)
{
  GstElement *pipeline = (GstElement *) data;

  switch (GST_MESSAGE_TYPE (message)) {
    case GST_MESSAGE_ERROR:{
        /* dump graph on error */
        GST_DEBUG_BIN_TO_DOT_FILE_WITH_TS (GST_BIN (pipeline),
                                           GST_DEBUG_GRAPH_SHOW_ALL, "error");

        print_error_message (message);

        /* we have an error */
        exit(1);
      }
    default:
      break;
  }
  return GST_BUS_PASS;
}


guint64 samplePts(GstSample* sample) {
  GstBuffer *buf = gst_sample_get_buffer(sample);
  return buf->pts;
}

string samplePtsString(GstSample* sample) {
  GstBuffer *buf = gst_sample_get_buffer(sample);
  char str[50];
  sprintf(str, "%" GST_TIME_FORMAT, GST_TIME_ARGS(buf->pts));
  return str;
}

vector<GstSample*> audioFrames;
vector<GstSample*> videoFrames;

GMainLoop* loop;

void demuxFile(vector<GstSample*>& out, const char* path) {
  GError *error = nullptr;
  GstElement *pipeline = gst_parse_launch("filesrc name=filesrc ! qtdemux ! appsink name=appsink async=false sync=false", &error);
  g_assert_no_error(error);

  GstBus *bus = gst_element_get_bus (pipeline);
  gst_bus_set_sync_handler (bus, bus_sync_handler, (gpointer) pipeline, NULL);
  gst_object_unref (bus);

  GstElement* filesrc = gst_bin_get_by_name(GST_BIN(pipeline), "filesrc");
  g_object_set(filesrc, "location", path, NULL);
  GstElement* appsink = gst_bin_get_by_name(GST_BIN(pipeline), "appsink");
  g_assert(appsink);

  GstStateChangeReturn ret = gst_element_set_state(pipeline, GST_STATE_PLAYING);
  g_assert(ret == GST_STATE_CHANGE_SUCCESS);

  GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "demux");

  GstSample* sample;
  while ((sample = gst_app_sink_pull_sample(GST_APP_SINK(appsink)))) {
    out.push_back(sample);
  }
  sort(out.begin(), out.end(), [](GstSample* a, GstSample* b) {
    return gst_sample_get_buffer(a)->dts < gst_sample_get_buffer(b)->dts;
  });

  gst_element_set_state(pipeline, GST_STATE_NULL);
  gst_object_unref(pipeline);
  g_printerr("%s: Demuxed %d frames.\n", path, (int) out.size());
}

vector<GstSample*> samplesStartingAt(const vector<GstSample*>& samples, guint64 start_pts) {
  vector<GstSample*> ret;
  int latestSyncFrameIndex = 0;
  for (size_t i = 0; i < samples.size(); i++) {
    GstBuffer *buf = gst_sample_get_buffer(samples[i]);
    guint64 pts = buf->pts;
    guint64 dur = buf->duration;
    guint64 pts_end = pts + dur;

    if (!(gst_buffer_get_flags(buf) & GST_BUFFER_FLAG_DELTA_UNIT)) {
      latestSyncFrameIndex = i;
    }

    if (pts_end >= start_pts) {
      ret.insert(ret.begin(), samples.begin() + latestSyncFrameIndex, samples.end());
      g_assert(!(gst_buffer_get_flags(gst_sample_get_buffer(ret[0])) & GST_BUFFER_FLAG_DELTA_UNIT));
      return ret;
    }
  }
  throw runtime_error("no frames in range");
}

void wait_for_pad (GstElement *element, string pad_name, function<void(GstElement* element, GstPad* pad)> callback)
{
  GstPad* pad = gst_element_get_static_pad(element, pad_name.c_str());
  if (pad) {
    callback(element, pad);
  } else {
    struct _pad_added_closure {
      string pad_name;
      function<void(GstElement* element, GstPad* pad)> callback;
    };

    _pad_added_closure *closure = new _pad_added_closure;
    closure->pad_name = pad_name;

    g_signal_connect(element, "pad-added", G_CALLBACK(+[](GstElement *element, GstPad *new_pad, gpointer user_data) -> void {
      _pad_added_closure *closure = (_pad_added_closure*) user_data;
      if (closure->pad_name == gst_pad_get_name(new_pad)) {
        closure->callback(element, new_pad);
        g_signal_handlers_disconnect_by_data(element, closure);
        delete closure;
      }
    }), closure);
  }
}

void wait_for_element (GstBin* bin, string element_name, function<void(GstBin* bin, GstElement* element)> callback)
{
  GstElement* element = gst_bin_get_by_name(GST_BIN(bin), element_name.c_str());
  if (element) {
    callback(bin, element);
  } else {
    struct _deep_element_added_closure {
      string element_name;
      function<void(GstBin* bin, GstElement* element)> callback;
    };

    _deep_element_added_closure *closure = new _deep_element_added_closure;
    closure->element_name = element_name;
    closure->callback = callback;

    g_signal_connect(bin, "deep-element-added", G_CALLBACK(+[](GstBin *bin, GstBin *sub_bin, GstElement *element, gpointer user_data) -> void {
      _deep_element_added_closure *closure = (_deep_element_added_closure *) user_data;
      if (closure->element_name == gst_element_get_name(element)) {
        closure->callback(bin, element);
        g_signal_handlers_disconnect_by_data(bin, closure);
        delete closure;
      }
    }), (void*)closure);
  }
}

void wait_for_element_and_pad (GstBin* bin, string element_name, string pad_name, std::function<void(GstElement* element, GstPad* pad)> callback)
{
  wait_for_element(bin, element_name, [pad_name, callback](GstBin* bin, GstElement* element) {
    wait_for_pad(element, pad_name, callback);
  });
}

typedef function<GstPadProbeReturn(GstPad *pad, GstPadProbeInfo *info)> ProbeFunction;

gulong pad_add_probe(GstPad *pad, GstPadProbeType mask, ProbeFunction callback)
{
  struct _closure {
    ProbeFunction callback;
  };
  _closure *closure = new _closure;
  closure->callback = callback;

  return gst_pad_add_probe(pad, mask, [](GstPad *pad, GstPadProbeInfo *info, gpointer user_data) -> GstPadProbeReturn {
    _closure *closure = (_closure*) user_data;
    return closure->callback(pad, info);
  }, closure, [](gpointer user_data) {
    _closure *closure = (_closure*) user_data;
    delete closure;
  });
}

void hook_probe_deferred (GstBin *bin, const char* element_name, const char* pad_name, GstPadProbeType mask, ProbeFunction probe_function)
{
  wait_for_element_and_pad(bin, element_name, pad_name, [mask, probe_function](GstElement* element, GstPad* pad) {
    pad_add_probe(pad, mask, probe_function);
  });
}


GstPadProbeReturn
debug_printer_probe (GstPad *pad, GstPadProbeInfo *info)
{
  g_assert(info->type & GST_PAD_PROBE_TYPE_BUFFER);
  GstBuffer *buf = (GstBuffer*) info->data;
  g_printerr("%s:%-10s:debug_printer_probe: buffer DTS=%" GST_TIME_FORMAT " PTS=%" GST_TIME_FORMAT " %s\n",
             gst_element_get_name(gst_pad_get_parent_element(pad)), gst_pad_get_name(pad),
             GST_TIME_ARGS(buf->dts), GST_TIME_ARGS(buf->pts),
             GST_BUFFER_FLAG_IS_SET(buf, GST_BUFFER_FLAG_DELTA_UNIT) ? "delta" : "sync");
  return GST_PAD_PROBE_OK;
}

void hook_debug_probe (GstBin *bin, const char* element_name, const char* pad_name)
{
  hook_probe_deferred(bin, element_name, pad_name, GST_PAD_PROBE_TYPE_BUFFER, debug_printer_probe);
}

guint global_group_id = -1;

GstPadProbeReturn change_group_id_probe (GstPad *pad, GstPadProbeInfo *info, gpointer user_data) {
  g_assert(info->type & GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM);
  GstEvent *event = (GstEvent*)info->data;
  if (event->type == GST_EVENT_STREAM_START) {
    GstEvent *new_event = gst_event_copy(event);
    gst_event_set_group_id(new_event, global_group_id);
    guint old_group_id = -1;
    gst_event_parse_group_id(event, &old_group_id);
    g_printerr("Changing group_id from %u to %u\n", old_group_id, global_group_id);
    gst_event_replace((GstEvent**)&info->data, new_event);
  }
  return GST_PAD_PROBE_OK;
}

struct BufferBarrierProbeData {
  guint64 pts;
  Barrier* barrier;
};

guint64 parseUnsignedTime(const char *str)
{
  int hours, minutes, seconds;
  long ns;
  int count_parsed_items = sscanf(str, "%d:%d:%d.%ld", &hours, &minutes, &seconds, &ns);
  g_assert(count_parsed_items == 4);
  return ns + GST_SECOND * (seconds + 60 * minutes + 3600 * hours);
}

vector<GstSample*> samplesBetweenInclusive(const vector<GstSample*>& allSamples, const char *firstSamplePTS, const char *lastSamplePTS) {
  vector<GstSample*> ret;
  guint64 firstSamplePTSns = parseUnsignedTime(firstSamplePTS);
  guint64 lastSamplePTSns = parseUnsignedTime(lastSamplePTS);

  auto iteratorFirst = std::find_if(allSamples.begin(), allSamples.end(), [firstSamplePTSns](GstSample* s) {
      return gst_sample_get_buffer(s)->pts == firstSamplePTSns;
  });
  g_assert(iteratorFirst != allSamples.end());

  auto iteratorLast = std::find_if(allSamples.begin(), allSamples.end(), [lastSamplePTSns](GstSample* s) {
      return gst_sample_get_buffer(s)->pts == lastSamplePTSns;
  });
  g_assert(iteratorFirst != allSamples.end());

  ret.insert(ret.end(), iteratorFirst, iteratorLast + 1);
  return ret;
}

GstPadProbeReturn segmentFixerProbe(GstPad*, GstPadProbeInfo* info, gpointer)
{
  GstEvent* event = GST_EVENT(info->data);

  if (GST_EVENT_TYPE(event) != GST_EVENT_SEGMENT)
    return GST_PAD_PROBE_OK;

  GstSegment* segment = nullptr;
  gst_event_parse_segment(event, const_cast<const GstSegment**>(&segment));

//  GST_TRACE("Fixed segment base time from %" GST_TIME_FORMAT " to %" GST_TIME_FORMAT,
//            GST_TIME_ARGS(segment->base), GST_TIME_ARGS(segment->start));

  // TODO How did this work on WebKit?
//  segment->base = segment->start;
  segment->base = segment->start - 3600 * GST_SECOND;
  segment->flags = static_cast<GstSegmentFlags>(0);

  return GST_PAD_PROBE_REMOVE;
}

int main(int argc, char** argv) {
  gst_init(&argc, &argv);

  GST_DEBUG_CATEGORY_INIT (testcase_debug, "testcase", 0, "testcase log");

  loop = g_main_loop_new (NULL, FALSE);

#if defined(ALICIA_DESKTOP)
  demuxFile(audioFrames, "/home/ntrrgc/Downloads/append-data/audio-full.mp4");
  demuxFile(videoFrames, "/home/ntrrgc/Downloads/append-data/video-full.mp4");
#elif defined(QUIQUE_BOARD)
  demuxFile(audioFrames, "/root/append-0.mp4");
  demuxFile(videoFrames, "/root/append-1.mp4");
#else
#error "Please specify environment in order to find the correct video files."
#endif

  GError *error = nullptr;
  GstElement *pipeline = gst_parse_launch("playsink name=playsink flags=audio+video+native-audio+native-video "
                                          "appsrc name=appsrcvideo ! queue name=myvideoqueue ! decodebin ! playsink.video_sink "
                                          "appsrc name=appsrcaudio ! queue name=myaudioqueue ! decodebin ! playsink.audio_sink ", &error);
  g_assert_no_error(error);

  GstBus *bus = gst_element_get_bus (pipeline);
  gst_bus_set_sync_handler (bus, bus_sync_handler, (gpointer) pipeline, NULL);
  gst_object_unref (bus);

  GstElement* appsrcvideo = gst_bin_get_by_name(GST_BIN(pipeline), "appsrcvideo");
  GstElement* appsrcaudio = gst_bin_get_by_name(GST_BIN(pipeline), "appsrcaudio");
//  hook_debug_probe(GST_BIN(pipeline), "avdec_h264-0", "sink");

  GstAppSrcCallbacks callbacks;
  callbacks.need_data = [](GstAppSrc *src, guint length, gpointer user_data) {
    //g_printerr("need-data %p\n", (void*) src);
  };
  callbacks.enough_data = [](GstAppSrc *src, gpointer user_data) {
    // g_printerr("enough-data %p\n", (void*) src);
  };
  callbacks.seek_data = [](GstAppSrc *src, guint64 offset, gpointer user_data) -> gboolean {
    g_printerr("seek-data %p offset=%" GST_TIME_FORMAT "\n", (void*) src, GST_TIME_ARGS(offset));
    return TRUE;
  };

  gst_app_src_set_callbacks(GST_APP_SRC(appsrcvideo), &callbacks, NULL, NULL);
  gst_app_src_set_stream_type(GST_APP_SRC(appsrcvideo), GST_APP_STREAM_TYPE_SEEKABLE);
  g_object_set(appsrcvideo, "format", GST_FORMAT_TIME, NULL);

  gst_app_src_set_callbacks(GST_APP_SRC(appsrcaudio), &callbacks, NULL, NULL);
  gst_app_src_set_stream_type(GST_APP_SRC(appsrcaudio), GST_APP_STREAM_TYPE_SEEKABLE);
  g_object_set(appsrcaudio, "format", GST_FORMAT_TIME, NULL);

  global_group_id = gst_util_group_id_next();
  gst_pad_add_probe(gst_element_get_static_pad(appsrcvideo, "src"), GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM, change_group_id_probe, NULL, NULL);
  gst_pad_add_probe(gst_element_get_static_pad(appsrcaudio, "src"), GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM, change_group_id_probe, NULL, NULL);

  GstStateChangeReturn ret;
  g_printerr("change to ready\n");
  ret = gst_element_set_state(pipeline, GST_STATE_READY);
  g_assert(ret != GST_STATE_CHANGE_FAILURE);
  GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "pipeline-ready");
  g_assert(ret == GST_STATE_CHANGE_SUCCESS);

  GstSegment segment;
  gst_segment_init(&segment, GST_FORMAT_TIME);
  segment.start = 0;
  g_printerr("initial seek to %" GST_TIME_FORMAT "\n", GST_TIME_ARGS(segment.start));
  gboolean seekSuccess;
  // Initial seek must be sent to the elements, not the pipeline, because events (including SEEK) don't flow in READY state.
  // For the same reason, we should not use GST_SEEK_FLAG_FLUSH: In READY state not even the FLUSH can flow. There are no
  // frames in the pipeline either.
  seekSuccess = gst_element_seek(appsrcaudio, 1, GST_FORMAT_TIME, GST_SEEK_FLAG_NONE,
                                 GST_SEEK_TYPE_SET, segment.start, GST_SEEK_TYPE_SET, GST_CLOCK_TIME_NONE);
  g_assert(seekSuccess);
  seekSuccess = gst_element_seek(appsrcvideo, 1, GST_FORMAT_TIME, GST_SEEK_FLAG_NONE,
                                 GST_SEEK_TYPE_SET, segment.start, GST_SEEK_TYPE_SET, GST_CLOCK_TIME_NONE);
  g_assert(seekSuccess);

  g_printerr("change to paused\n");
  ret = gst_element_set_state(pipeline, GST_STATE_PAUSED);
  g_printerr("change to paused returned\n");
  GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "pipeline-paused");
  g_assert(ret != GST_STATE_CHANGE_FAILURE);

  g_printerr("change to playing\n");
  ret = gst_element_set_state(pipeline, GST_STATE_PLAYING);
  g_printerr("change to playing returned\n");
  GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "pipeline-playing");
  g_assert(ret != GST_STATE_CHANGE_FAILURE);

  g_printerr("appends 1 (bad frames)\n");
  for (auto sample : samplesBetweenInclusive(videoFrames, "1:00:00.033333333", "1:00:21.333333333")) {
    gst_app_src_push_sample(GST_APP_SRC(appsrcvideo), sample);
  }
  for (auto sample : samplesBetweenInclusive(audioFrames, "0:59:50.013968253", "1:00:18.946031746")) {
    gst_app_src_push_sample(GST_APP_SRC(appsrcaudio), sample);
  }

  GstState reachedState;
  // Wait for PLAYING transition to complete in order to avoid GStreamer bug if a PAUSED transition is requested.
  gst_element_get_state(pipeline, &reachedState, NULL, GST_CLOCK_TIME_NONE);
  g_assert(reachedState == GST_STATE_PLAYING);

  sleep(1);
  GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "playing");

  // Seek
  ret = gst_element_set_state(pipeline, GST_STATE_PAUSED);
  GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "pause-before-seek");
  g_assert(ret != GST_STATE_CHANGE_FAILURE);

  g_printerr("paused\n");

  gst_element_get_state(pipeline, &reachedState, NULL, GST_CLOCK_TIME_NONE);
  g_assert(reachedState == GST_STATE_PAUSED);

  g_printerr("paused reached, now seeking\n");
  
  segment.start = parseUnsignedTime("1:00:00.000000000");

  for (int i=0; i<5; i++) {
    seekSuccess = gst_element_seek(pipeline, 1, GST_FORMAT_TIME, GST_SEEK_FLAG_FLUSH,
                                   GST_SEEK_TYPE_SET, segment.start, GST_SEEK_TYPE_SET, GST_CLOCK_TIME_NONE);
    g_assert(seekSuccess);

    ret = gst_element_set_state(pipeline, GST_STATE_PLAYING);
    GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "play-after-seek");
    g_assert(ret != GST_STATE_CHANGE_FAILURE);

    g_printerr("appends %d\n", i+2);
    auto videoFramesSeek = samplesStartingAt(videoFrames, segment.start);
    g_assert(samplePtsString(videoFramesSeek[0]) == "1:00:00.033333333");
    for (auto sample : videoFramesSeek) {
      gst_app_src_push_sample(GST_APP_SRC(appsrcvideo), sample);
    }
    auto audioFramesSeek = samplesStartingAt(audioFrames, segment.start);
    g_assert(samplePtsString(audioFramesSeek[0]) == "0:59:59.998548752");
    for (auto sample : samplesStartingAt(audioFrames, segment.start)) {
      gst_app_src_push_sample(GST_APP_SRC(appsrcaudio), sample);
    }

    sleep(2);

    g_printerr("Flush + reenqueue\n");

    gint64 position = GST_CLOCK_TIME_NONE;
    GstQuery* query = gst_query_new_position(GST_FORMAT_TIME);
    if (gst_element_query(pipeline, query))
        gst_query_parse_position(query, 0, &position);

    GST_TRACE("Position: %" GST_TIME_FORMAT, GST_TIME_ARGS(position));

    if (!GST_CLOCK_TIME_IS_VALID(position)) {
        GST_TRACE("Can't determine position, avoiding flush");
        abort();
    }

    double rate;
    GstFormat format;
    gint64 start = GST_CLOCK_TIME_NONE;
    gint64 stop = GST_CLOCK_TIME_NONE;

    gst_query_unref(query);
    query = gst_query_new_segment(GST_FORMAT_TIME);
    if (gst_element_query(pipeline, query))
      gst_query_parse_segment(query, &rate, &format, &start, &stop);

    GST_TRACE("segment: [%" GST_TIME_FORMAT ", %" GST_TIME_FORMAT "], rate: %f",
      GST_TIME_ARGS(start), GST_TIME_ARGS(stop), rate);

    if (!gst_element_send_event(GST_ELEMENT(appsrcvideo), gst_event_new_flush_start())) {
      GST_ERROR("Failed to send flush-start");
      abort();
    }

    if (!gst_element_send_event(GST_ELEMENT(appsrcvideo), gst_event_new_flush_stop(false))) {
      GST_ERROR("Failed to send flush-stop");
      abort();
    }

    if (static_cast<guint64>(position) == GST_CLOCK_TIME_NONE || static_cast<guint64>(start) == GST_CLOCK_TIME_NONE) {
      GST_ERROR("position or start is NONE");
      abort();
    }

    GstSegment* segment(gst_segment_new());
    gst_segment_init(segment, GST_FORMAT_TIME);
    gst_segment_do_seek(segment, rate, GST_FORMAT_TIME, GST_SEEK_FLAG_NONE,
      GST_SEEK_TYPE_SET, position, GST_SEEK_TYPE_SET, stop, nullptr);

    GstPad* appsrcvideoSrc = gst_element_get_static_pad(appsrcvideo, "src");
    GstPad* appsrcvideoSrcPeer = gst_pad_get_peer(appsrcvideoSrc);
    gst_pad_add_probe(appsrcvideoSrcPeer, GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM, segmentFixerProbe, NULL, NULL);

    GST_TRACE("Sending new seamless segment: [%" GST_TIME_FORMAT ", %" GST_TIME_FORMAT "], rate: %f",
        GST_TIME_ARGS(segment->start), GST_TIME_ARGS(segment->stop), segment->rate);

    if (!gst_base_src_new_seamless_segment(GST_BASE_SRC(appsrcvideo), segment->start, segment->stop, segment->start)) {
        GST_WARNING("Failed to send seamless segment event");
        abort();
    }

    GST_DEBUG("flushed video, reenqueueing new frames");

    for (auto sample : samplesStartingAt(videoFrames, segment->start)) {
      gst_app_src_push_sample(GST_APP_SRC(appsrcvideo), sample);
    }

    GST_DEBUG("reenqueued new frames");

    sleep(40);
  }

  return 0;
}
