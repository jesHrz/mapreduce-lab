<template>
  <div>
    <div class="search">
      <input type="text" v-model="searchUser" @keydown.enter="search"/>
      <button @click="search">
        <span class="el-icon-user"></span>
      </button>
    </div>
    <div>
      <div class="tip" v-if="searchResult.length === 0">No response</div>
      <div class="tip" v-else>He/She has listened to the songs of {{ searchResult.length }} artists. Try it!</div>
      <PlayList :data="searchResult" v-loading="!searched"></PlayList>
    </div>
  </div>
</template>

<script>
import PlayList from "@/components/PlayList.vue";
import { get } from "@/utils/api.js";
export default {
  components: { PlayList },
  data() {
      return {
          searchUser: "",
          searchResult: [],
          searched: true
      }
  },
  methods: {
    async search() {
      this.searched = false;
      const ret = await get("/history?user=", 8080, this.searchUser);
      this.searchResult = ret;
      this.searched = true;
    }
  }
};
</script>

<style>
.tip {
  text-align: left;
  margin: 20px auto;
  font-weight: bold;
  font-size: x-large;
}
.search {
  overflow: hidden;
  position: relative;
  width: 400px;
  margin: 20px auto;
}
.search input {
  display: inline-block;
  height: 40px;
  width: 200px;
  vertical-align: middle;
  border: none;
  border-bottom: 1px solid #d3d3d3;
  outline: none;
  padding: 10px;
}
.search button {
  background: #fff;
  display: inline-block;
  vertical-align: middle;
  outline: none;
  width: 60px;
  height: 40px;
  border: none;
  border-bottom: 1px solid #d3d3d3;
}
</style>