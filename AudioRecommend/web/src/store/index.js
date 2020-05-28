import Vue from 'vue';
import Vuex from 'vuex';
import user from '@/store/modules/user.js';
import audio from '@/store/modules/audio.js';

Vue.use(Vuex);

const store = new Vuex.Store({
    modules: {
        user,
        audio
    }
});

export default store;