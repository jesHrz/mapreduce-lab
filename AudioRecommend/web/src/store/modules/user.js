import Vue from 'vue';
import { get } from '@/utils/api.js';


const user = {
  namespaced: true,
  state: {
    user: -1,
    recommendData: [],
    historyData: [],
  },
  mutations: {
    login(state, payload) {
      state.user = payload.user;
    },
    updateRecommendData(state, payload) {
      state.recommendData = payload.recommend;
      state.recommendData.sort((x1, x2) => {
        return x2._2 - x1._2;
      })
    },
    updateHistoryData(state, payload) {
      state.historyData = payload.history;
    }
  },
  getters: {
    currentUser: state => state.user
  },
  actions: {
    async login(context, payload) {
      try {
        const ret = await get('/count?user=', 8080, payload.user);
        if (ret != 0) {
          context.commit("login", payload);
          Vue.prototype.$success("Generating recommendation...");
          context.commit("updateRecommendData", { 
            recommend: await get('/recommend?user=', 8080, payload.user)
          });
          context.commit("updateHistoryData", { 
            history: await get('/history?user=', 8080, payload.user)
          });
          Vue.prototype.$success("Welcome back! " + payload.user);
        } else {
          Vue.prototype.$error("No response");
        }
      } catch (err) {
        Vue.prototype.$error(err);
      }
      payload.loading.close();
    },
  }
}

export default user;