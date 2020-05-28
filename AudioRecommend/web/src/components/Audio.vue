<template>
  <div class="audio">
    <audio id="music" :src="songUrl" ref="audio" autoplay></audio>
    <div class="webapp-controls clearfix">
      <div class="audio-avatar">
        <img :src="picUrl" />
      </div>
      <div class="audio-info" @click="showLyric=!showLyric">
        <p>{{ songName }}</p>
        <p>{{ singer }}</p>
        <p>{{ currentTime }} / {{ duration }}</p>
      </div>
      <div class="audio-operation">
        <span @click="setPlayStatus" :class="isPlay ? 'el-icon-video-pause' : 'el-icon-video-play'"></span>
      </div>
    </div>

    <div class="lyricList" :class="showLyric?'show':'none'">
      <ul class="lyric" ref="playerLyric">
        <li ref="playerLyricli"></li>
        <li></li>
        <li
          v-for="(l, index) in lyric"
          :key="index"
          :class="[index+1===lyricIndex?'active':'']"
        >{{l.lyr}}</li>
        <li></li>
        <li></li>
      </ul>
    </div>
  </div>
</template>

<script>
import defaultPicUrl from "@/assets/default-cover.svg";
import { mapState, mapActions } from "vuex";
import { s_2_hs } from "@/utils/tools.js";

export default {
  data: function() {
    return {
      showLyric: false
    };
  },
  methods: {
    ...mapActions("audio", [
      "setLyric",
      "setAudio",
      "setCurrentTime",
      "setDuration",
      "setIsPlay",
      "updateLyricIndex"
    ]),
    getSongUrl(id) {
      this.setLyric(id);
      return `https://music.163.com/song/media/outer/url?id=${id}.mp3`;
    },
    setPlayStatus() {
      this.$nextTick(() => {
        let audio = document.getElementById("music");
        if (audio.paused) {
          audio.play();
          this.setIsPlay(true);
        } else {
          audio.pause();
          this.setIsPlay(false);
        }
      });
    }
  },
  computed: {
    ...mapState("audio", {
      _currentTime: state => state.currentTime,
      duration: state => {
        return s_2_hs(state.duration);
      }
    }),
    ...mapState("audio", ["song", "isPlay", "lyric", "lyricIndex"]),
    songName() {
      if (Object.prototype.hasOwnProperty.call(
          this.song,
          "name"
        )) {
        return this.song.name;
      } else {
        return "No song";
      }
    },
    songUrl() {
      if (Object.prototype.hasOwnProperty.call(
          this.song,
          "id"
        )) {
        return this.getSongUrl(this.song.id);
      } else {
        return "";
      }
    },
    singer() {
      if (Object.prototype.hasOwnProperty.call(
          this.song,
          "ar"
        )) {
        return this.song.ar[0].name;
      } else {
        return "null";
      }
    },
    picUrl() {
      if (Object.prototype.hasOwnProperty.call(
          this.song,
          "al"
        )) {
        return this.song.al.picUrl;
      } else {
        return defaultPicUrl;
      }
    },
    currentTime() {
      if (
        this.lyric.length != 0 &&
        this.lyric[this.lyricIndex] &&
        Object.prototype.hasOwnProperty.call(
          this.lyric[this.lyricIndex],
          "time"
        )
      ) {
        if (this._currentTime >= this.lyric[this.lyricIndex].time) {
          this.updateLyricIndex({
            value: 1,
            object: this
          });
          
        }
      }
      return s_2_hs(this._currentTime);
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
  padding: 0 540px 0 10px;
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

.audio .lyricList {
  position: fixed;
  animation: showList 1s;
  -webkit-animation: showList 1s;
  width: 700px;
  height: 60vh;
  bottom: 60px;
  background: #fff;
  border-top: 1px solid #42b983;
}
.audio .lyricList.show {
  display: block;
}
.audio .lyricList.none {
  display: none;
}
.audio .lyricList .close {
  float: right;
}

@-webkit-keyframes showList {
  0% {
    height: 0;
  }
  to {
    height: 60vh;
  }
}
@keyframes showList {
  0% {
    height: 0;
  }
  to {
    height: 60vh;
  }
}

.audio .lyric {
  height: 50vh;
  overflow-x: scroll;
  margin: 50px 0;
}
.audio .lyric li {
  height: 20%;
  line-height: 8vh;
  color: #373737;
  font-weight: 400;
  font-size: 14px;
}
.audio .lyric li.active {
  color: #42b983;
}
</style>