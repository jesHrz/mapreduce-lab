<template>
  <div class="navHeader">
    <ul class="nav">
            <li>
              <router-link to="/" v-if="user === -1">
                Home
              </router-link>
              <router-link to="/" v-else>
                {{ user }}
              </router-link>
            </li>
            <li><router-link to="/search">Search</router-link> </li>
            <li><router-link to="/artist">Statistic</router-link> </li>
    </ul>
  </div>
</template>

<script>
import { mapState } from "vuex";

export default {
  computed: {
    ...mapState('user', ["user"])
  }
};
</script>

<style>
.navHeader {
  position: sticky;
  padding: 10px;
  border-bottom: 1px solid #5CBB7A;
  top: 0;
  background: #fff;
  z-index: 1;
}
.navHeader a {
  font-weight: bold;
  color: #2c3e50;
  text-decoration: none;
}
.navHeader a.router-link-exact-active {
  color: #5CBB7A;
}
.navHeader .nav{
    list-style: none;
}
.navHeader .nav li{
    display: inline-block;
    padding: 10px 20px;
}
</style>