<template>
  <div id="app" class="main_div">
    <Header></Header>
    <router-view id="main" />
    <Audio></Audio>
  </div>
</template>

<script>
import Header from '@/components/Header.vue'
import Audio from '@/components/Audio.vue'

export default {
  name: 'App',
  components: {
    Header,
    Audio
  }
}
</script>

<style>
*{
  padding: 0;
  margin: 0;
  box-sizing: border-box;
}
.clearfix::after{
    content: '';
    display: block;
    clear: both;
}
#main{
  margin-bottom: 70px;
}
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
.main_div {
  /* width: 50%; */
  max-width: 700px;
  /* min-height: 720px; */
  margin: 0px auto 0 auto;
}
</style>
