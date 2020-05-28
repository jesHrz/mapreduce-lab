import axios from 'axios'
import Vue from 'vue'

Vue.prototype.$http = axios;
const baseUrl = "//localhost";

export function get(path, port=3000, params) {
    console.log(`${baseUrl}:${port}${path}${params}`);
    return new Promise((resolve, reject) => {
      axios
        .get(`${baseUrl}:${port}${path}${params}`)
        .then(ret => {
          console.log(ret.data);
          resolve(ret.data);
        })
        .catch(err => {
          reject(err);
        });
    });
  }