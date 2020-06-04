<template>
  <div>
    <el-dialog title="Who are you?" :visible="currentUser == -1" width="30%" center>
      <el-input placeholder="User ID" v-model="user" @keydown.enter="entry"></el-input>
      <span slot="footer">
        <el-button type="primary" @click="entry">OK</el-button>
      </span>
    </el-dialog>

    <div class="recommend">
      <div class="tip">Daily Top 10</div>
      <PlayList :data="recommendData"></PlayList>
    </div>

    <div>
      <div class="tip">You have listened to the songs of {{ historyData.length }} artists</div>
      <Chart
        v-if="historyData.length <= 15"
        ref="chart"
        :chartStyle="'margin: 20px; width: 600px; height: 400px;'"
      ></Chart>
      <PlayList :data="historyData"></PlayList>
    </div>
  </div>
</template>

<script>
import { mapState, mapActions, mapGetters } from "vuex";
import { Loading } from "element-ui";
import PlayList from "@/components/PlayList.vue";
import Chart from "@/components/Chart.vue";
export default {
  name: "Home",
  components: { PlayList, Chart },
  data() {
    return {
      user: "",
      loading: false
    };
  },
  methods: {
    ...mapActions("user", ["login"]),
    entry() {
      this.loading = Loading.service();
      this.login(this);
      // this.$store.dispatch("login", this);
    }
  },
  mounted() {
      if(this.historyData.length > 0 && this.historyData.length <= 15) {
        const tmp = this.historyData.map(x => {
          return { name: x['_1'], value: x['_2'] }
        })
        this.$refs.chart.drawPie(
            'Statistic of ' + this.user,
            tmp.map(x => x.name),
            tmp
        )
      }
  },
  computed: {
    ...mapState("user", ["recommendData", "historyData"]),
    ...mapGetters("user", ["currentUser"])
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