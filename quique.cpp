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
#include <string.h>
using namespace std;

struct EndOfAppendMeta {
  GstMeta base;

  static gboolean init(GstMeta*, void*, GstBuffer*)
  {
    return TRUE;
  }

  static gboolean transform(GstBuffer*, GstMeta*, GstBuffer*, GQuark, void*)
  {
    assert(0);
  }

  static void free(GstMeta*, GstBuffer*)
  {
    // No pointers to free.
  }
};

static GType WEBKIT_END_OF_APPEND_META_API_TYPE = 0;
static const GstMetaInfo* webKitEndOfAppendMetaInfo = nullptr;

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

GstPadProbeReturn
debug_printer_probe (GstPad *pad, GstPadProbeInfo *info, gpointer user_data)
{
  g_assert(info->type & GST_PAD_PROBE_TYPE_BUFFER);
  GstBuffer *buf = (GstBuffer*) info->data;
  g_printerr("%s:%-10s:debug_printer_probe: buffer DTS=%" GST_TIME_FORMAT " PTS=%" GST_TIME_FORMAT " %s\n",
             gst_element_get_name(gst_pad_get_parent_element(pad)), gst_pad_get_name(pad),
             GST_TIME_ARGS(buf->dts), GST_TIME_ARGS(buf->pts),
             GST_BUFFER_FLAG_IS_SET(buf, GST_BUFFER_FLAG_DELTA_UNIT) ? "delta" : "sync");
  return GST_PAD_PROBE_OK;
}

void _hook_debug_probe_there_is_pad(GstElement* element, GstPad* pad)
{
  gst_pad_add_probe(pad, GST_PAD_PROBE_TYPE_BUFFER, debug_printer_probe, NULL, NULL);
}

void _hook_debug_probe_there_is_element(GstElement* element, const char* pad_name)
{
  char* disconnect_handle= new char;
  GstPad* pad = gst_element_get_static_pad(element, pad_name);
  if (pad) {
    _hook_debug_probe_there_is_pad(element, pad);
  } else {
    g_signal_connect(element, "pad-added", G_CALLBACK(+[](GstElement *element, GstPad *new_pad, gpointer user_data) {
      char* disconnect_handle = (char*) user_data;
      _hook_debug_probe_there_is_pad(element, new_pad);
      g_signal_handlers_disconnect_by_data(element, disconnect_handle);
    }), disconnect_handle);
  }
}

struct _hook_debug_probe_deep_element_added_closure {
  const char* element_name;
  const char* pad_name;
};

void hook_debug_probe (GstElement *pipeline, const char* element_name, const char* pad_name)
{
  GstElement* element = gst_bin_get_by_name(GST_BIN(pipeline), element_name);
  if (element) {
    _hook_debug_probe_there_is_element(element, pad_name);

  } else {
    _hook_debug_probe_deep_element_added_closure *closure = new _hook_debug_probe_deep_element_added_closure;
    closure->element_name = element_name;
    closure->pad_name = pad_name;

    g_signal_connect(pipeline, "deep-element-added", G_CALLBACK(+[](GstBin     *bin,
                     GstBin     *sub_bin,
                     GstElement *element,
                     gpointer    user_data)
    {
      _hook_debug_probe_deep_element_added_closure *closure = (_hook_debug_probe_deep_element_added_closure *) user_data;
      if (!strcmp(closure->element_name, gst_element_get_name(element))) {
       _hook_debug_probe_there_is_element(element, closure->pad_name);
       delete closure;
       g_signal_handlers_disconnect_by_data(bin, closure);
      }
    }), (void*)closure);
  }
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

int main(int argc, char** argv) {
  gst_init(&argc, &argv);
  const char* tags[] = {nullptr};
  WEBKIT_END_OF_APPEND_META_API_TYPE = gst_meta_api_type_register("WebKitEndOfAppendMetaAPI", tags);
  webKitEndOfAppendMetaInfo = gst_meta_register(WEBKIT_END_OF_APPEND_META_API_TYPE, "WebKitEndOfAppendMeta", sizeof(EndOfAppendMeta), EndOfAppendMeta::init, EndOfAppendMeta::free, EndOfAppendMeta::transform);

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
//  hook_debug_probe(pipeline, "avdec_h264-0", "sink");

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
  segment.start = max(samplePts(videoFrames[0]), samplePts(audioFrames[0]));
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

  g_printerr("appends 1 (for initial seek)\n");
  for (auto sample : samplesStartingAt(videoFrames, segment.start)) {
    gst_app_src_push_sample(GST_APP_SRC(appsrcvideo), sample);
  }
  for (auto sample : samplesStartingAt(audioFrames, segment.start)) {
    gst_app_src_push_sample(GST_APP_SRC(appsrcaudio), sample);
  }

  sleep(2);
  GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "playing");

  // Seek
  ret = gst_element_set_state(pipeline, GST_STATE_PAUSED);
  GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "pause-before-seek");
  g_assert(ret != GST_STATE_CHANGE_FAILURE);

  g_printerr("paused\n");

  GstState reachedState;
  gst_element_get_state(pipeline, &reachedState, NULL, GST_CLOCK_TIME_NONE);
  g_assert(reachedState == GST_STATE_PAUSED);

  g_printerr("paused reached, now seeking\n");
  
  segment.start = 3615033333333;

  for (int i=0; i<5; i++) {
    seekSuccess = gst_element_seek(pipeline, 1, GST_FORMAT_TIME, GST_SEEK_FLAG_FLUSH,
                                   GST_SEEK_TYPE_SET, segment.start, GST_SEEK_TYPE_SET, GST_CLOCK_TIME_NONE);
    g_assert(seekSuccess);

    ret = gst_element_set_state(pipeline, GST_STATE_PLAYING);
    GST_DEBUG_BIN_TO_DOT_FILE(GST_BIN(pipeline), GST_DEBUG_GRAPH_SHOW_ALL, "play-after-seek");
    g_assert(ret != GST_STATE_CHANGE_FAILURE);

    g_printerr("appends %d\n", i+2);
    for (auto sample : samplesStartingAt(videoFrames, segment.start)) {
      gst_app_src_push_sample(GST_APP_SRC(appsrcvideo), sample);
    }
    for (auto sample : samplesStartingAt(audioFrames, segment.start)) {
      gst_app_src_push_sample(GST_APP_SRC(appsrcaudio), sample);
    }

    sleep(8);
  }

  return 0;
}
