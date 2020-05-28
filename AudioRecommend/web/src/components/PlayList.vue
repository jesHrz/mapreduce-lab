<template>
  <div>
    <el-collapse @change="handleCollapseChange" accordion>
      <el-collapse-item
        v-for="(item, index) in data"
        :key="index"
        :title="item['_1']"
        :name="item['_1']"
      >
        <ul class="play-list" v-loading="!searched">
          <li
            @click="setSong(song.id)"
            v-for="(song, index2) in searchResult"
            :key="index2"
            class="clearfix"
          >
            <span class="avatar">
              <img :src="song.album.artist.img1v1Url" alt="picUrl" />
            </span>
            <p>
              <span>{{song.name}}</span>
              <span>{{song.artists[0].name}}</span>
              <span>{{song.album.name}}</span>
            </p>
          </li>
        </ul>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>

<script>
import { get } from "@/utils/api.js";
import { mapActions } from "vuex";
export default {
  props: {
    data: {
      type: Array,
      default: () => []
    }
  },
  data: function() {
    return {
      searched: false,
      searchResult: []
    };
  },
  methods: {
    ...mapActions("audio", ["setSong"]),
    async handleCollapseChange(artistName) {
      this.searched = false;
      const ret = await get("/search?limit=5&keywords=", 3000, artistName);
      if (ret.code === 200) {
        this.searchResult = ret.result.songs;
      }
      this.searched = true;
    }
  }
};
</script>

<style>
.play-list {
  width: 90%;
  margin: 0 auto 0;
  list-style: none;
  border-bottom: 0.5px solid #d4d4d4;
}
.play-list li {
  border-top: 0.5px solid #d4d4d4;
  text-align: left;
  padding: 5px 0;
}
.play-list li:first-child {
  text-align: center;
  cursor: pointer;
}
.play-list a {
  color: #666;
  text-decoration: none;
}
.play-list .avatar {
  display: inline-block;
  width: 60px;
  height: 60px;
  padding: 5px;
  margin-right: 10px;
  vertical-align: middle;
}
.play-list img {
  display: inline-block;
  width: 50px;
  height: 50px;
}
.play-list p {
  display: inline-block;
  vertical-align: middle;
  font-size: 12px;
  height: 60px;
  text-align: left;
  width: calc(100% - 70px);
}
.play-list p span {
  display: block;
  height: 20px;
  line-height: 20px;
  font-weight: 100;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.play-list p span:first-child {
  font-weight: 700;
}
</style>