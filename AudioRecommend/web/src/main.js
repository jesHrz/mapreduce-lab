import Vue from 'vue'
import App from './App.vue'
import router from './router.js'
// import store from './store.js'
import store from '@/store/index.js'
import ElementUI from 'element-ui';
import 'element-ui/lib/theme-chalk/index.css';

Vue.use(ElementUI);
Vue.config.productionTip = false

Vue.prototype.$error = msg => {
    Vue.prototype.$message({ message: msg, type: 'error' });
};

Vue.prototype.$success = msg => {
    if (!msg) {
      Vue.prototype.$message({ message: 'Succeeded', type: 'success' });
    } else {
      Vue.prototype.$message({ message: msg, type: 'success' });
    }
};

new Vue({
  router,
  store,
  render: h => h(App),
}).$mount('#app')
