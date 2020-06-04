<template>
  <div>
    <div class="tip">Most popular artists of Top 10</div>
    <Chart
      ref="chart"
      :chartStyle="'margin: 20px; width: 600px; height: 400px;'"
      @chartClick="handleChartClick"
    ></Chart>
    <PlayList :data="searchResult" style="margin-top: 10px;"></PlayList>
  </div>
</template>

<script>
import { get } from "@/utils/api.js";
import PlayList from "@/components/PlayList.vue";
import Chart from "@/components/Chart.vue";
export default {
  components: { PlayList, Chart },
  data() {
    return {
      showChart: true,
      searchResult: []
    };
  },
  methods: {
    handleChartClick(data) {
      console.log(data);
      this.searchResult = [{ _1: data.data.name }];
    }
  },
  mounted() {
    get("/statistic?limit=", 8080, -1).then(ret => {
      ret = ret.map(x => {
        return {
          name: x['_1'],
          value: x['_2']
        }
      })
      ret.sort((x1, x2) => x2.value - x1.value);
      this.$refs.chart.drawPie(
        "Statistic of Artist",
        ret.slice(0, 10).map(x => x.name),
        ret.slice(0, 10)
      );
    });
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
</style>