<template>
  <div>
    <div class="search">
      <input type="text" v-model="searchUser" @keydown.enter="search" />
      <button @click="search">
        <span class="el-icon-user"></span>
      </button>
    </div>
    <div>
      <div class="tip" v-if="searchResult.length === 0">No response</div>
      <div
        class="tip"
        v-else
      >He/She has listened to the songs of {{ searchResult.length }} artists. Try it!</div>
      <Chart
        v-if="searchResult.length <= 15"
        ref="chart"
        v-loading="!searched"
        :chartStyle="'margin: 20px; width: 600px; height: 400px;'"
      ></Chart>
      <PlayList :data="searchResult" v-loading="!searched"></PlayList>
    </div>
  </div>
</template>

<script>
import PlayList from "@/components/PlayList.vue";
import Chart from "@/components/Chart.vue";
import { get } from "@/utils/api.js";
export default {
  components: { PlayList, Chart },
  data() {
    return {
      searchUser: "",
      searchResult: [],
      searched: true
    };
  },
  methods: {
    search() {
      this.searched = false;
      get("/history?user=", 8080, this.searchUser).then(ret => {
        this.searchResult = ret;
        ret = ret.map(x => {
          return { name: x["_1"], value: x["_2"] };
        });
        if (this.searchResult.length <= 15) {
          this.$refs.chart.drawPie(
            "Statistic of " + this.searchUser,
            ret.map(x => x.name),
            ret
          );
        }
      });
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