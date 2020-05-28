<template>
  <div class="audio">
    <audio id="music" :src="songUrl" ref="audio" autoplay></audio>
    <div class="webapp-controls clearfix">
      <div class="audio-avatar">
        <img :src="picUrl" />
      </div>
      <div class="audio-info" @click="drawer=true">
        <p>{{ songName }}</p>
        <p>{{ singer }}</p>
        <p>{{ currentTime }} / {{ duration }}</p>
      </div>
      <div class="audio-operation">
        <span @click="setPlayStatus" :class="isPlay ? 'el-icon-video-pause' : 'el-icon-video-play'"></span>
      </div>
    </div>

    <el-drawer
      direction="rtl"
      :visible.sync="drawer"
      :append-to-body="true"
      :modal="false"
      :show-close="false"
      :with-header="false"
    >
      <Player></Player>
    </el-drawer>
  </div>
</template>

<script>
import defaultPicUrl from "@/assets/default-cover.svg";
import Player from "@/components/Player.vue";
import { mapState, mapActions } from "vuex";
import { s_2_hs } from "@/utils/tools.js";

export default {
  components: { Player },
  data: function() {
    return {
      drawer: false,
    };
  },
  methods: {
    ...mapActions("audio", [
      "setLyric",
      "setAudio",
      "setCurrentTime",
      "setDuration",
      "setIsPlay"
    ]),
    getSongUrl(id) {
      this.setLyric(id);
      return `https://music.163.com/song/media/outer/url?id=${id}.mp3`;
    },
    setPlayStatus() {
      this.$nextTick(() => {
        let audio = document.getElementById("music");
        if(audio.paused)  {
          audio.play();
          this.setIsPlay(true);
        } else {
          audio.pause();
          this.setIsPlay(false);
        }
      })
    },
  },
  computed: {
    ...mapState("audio", {
      song: state => state.song,
      currentTime: state => {
        return s_2_hs(state.currentTime);
      },
      duration: state => {
        return s_2_hs(state.duration);
      },
      isPlay: state => state.isPlay
    }),
    songName() {
      if (this.song) {
        return this.song.name;
      } else {
        return "当前暂未播放歌曲";
      }
    },
    songUrl() {
      if (this.song) {
        return this.getSongUrl(this.song.id);
      } else {
        return "";
      }
    },
    singer() {
      if (this.song) {
        return this.song.ar[0].name;
      } else {
        return "暂无";
      }
    },
    picUrl() {
      if (this.song) {
        return this.song.al.picUrl;
      } else {
        return defaultPicUrl;
      }
    }
  },
  mounted() {
    const audio = this.$refs.audio;
    this.setAudio(audio);
    audio.addEventListener("timeupdate", () => {
      this.setCurrentTime(isNaN(audio.currentTime) ? 0 : audio.currentTime);
    });
    audio.addEventListener("loadeddata", () => {
      this.setDuration(isNaN(audio.duration) ? 0 : audio.duration);
    });
  }
};
</script>


<style>
.audio {
  /* padding-left: 50px; */
  position: fixed;
  border-top: 1px solid #f2f2f2;
  /* left: 0; */
  /* right: 0; */
  bottom: 0;
  height: 60px;
  background-color: #fff;
}
.audio .audio-avatar {
  position: relative;
  display: inline-block;
}
.audio .audio-avatar img {
  padding: 5px;
  width: 60px;
  border-radius: 50%;
}
.audio .audio-info {
  display: inline-block;
  /* width: 700px; */
  vertical-align: top;
  padding: 0 550px 0 10px;
}
.audio .audio-info p {
  height: 20px;
  line-height: 20px;
  font-size: 10px;
  text-align: left;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.audio .audio-info p:first-child {
  font-size: 10px;
}
.audio .audio-operation {
  display: inline-block;
  height: 60px;
  vertical-align: top;
}
.audio .audio-operation::before {
  content: "";
  display: inline-block;
  height: 100%;
  vertical-align: middle;
}
</style>