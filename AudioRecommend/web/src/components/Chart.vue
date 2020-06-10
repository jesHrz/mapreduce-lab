<template>
  <div>
    <button
      type="text"
      :class="showChart ? 'el-icon-arrow-up' : 'el-icon-arrow-down'"
      style="float: right;"
      @click="showChart=!showChart"
    ></button>
    <div id="_chart" :style="chartStyle" v-show="showChart"></div>
  </div>
</template>

<script>
import echarts from "echarts";
export default {
  props: {
    data: {
      type: Array,
      default: () => []
    },
    column: {
      type: Array,
      default: () => []
    },
    chartStyle: {
      type: String,
      default: () => ""
    }
  },
  data() {
    return {
      showChart: true
    }
  },
  methods: {
    drawPie(name, column, data) {
      this.charts = echarts.init(document.getElementById("_chart"));
      this.charts.on("click", data => {
        try{
          this.$emit("chartClick", data);
        } catch(err) {err;}
      });
      this.charts.setOption({
        tooltip: {
          trigger: "item"
        },
        legend: {
          orient: "vertical",
          x: "left",
          data: column
        },
        series: [
          {
            name: name,
            type: "pie",
            radius: ["50%", "70%"],
            avoidLabelOverlap: false,
            label: {
              normal: {
                show: false,
                position: "center"
              },
              emphasis: {
                show: true,
                textStyle: {
                  fontSize: "30",
                  fontWeight: "blod"
                }
              }
            },
            labelLine: {
              normal: {
                show: false
              }
            },
            data: data
          }
        ]
      });
    }
  }
};
</script>