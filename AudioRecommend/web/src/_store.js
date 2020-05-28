import Vue from 'vue'
import Vuex from 'vuex'
import { get } from './utils/api.js'
import { getLyric } from './utils/tools.js'

Vue.use(Vuex);

const PORT = 3000;

export default new Vuex.Store({
    state: {
        // 歌曲集合
        songs: [],
        ids: [],
        lyric: [],
        songNum: 0,
        currentTime: 0,
        duration: 0,
        audio: {},
        isPlay: true,
        upTime: 0,

        user: -1,
        recommendData: [],
        historyData: [],
    },
    getters: {
        user: state => { return state.user }
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
        },
        getSongs(state, payload) {
            state.songs = payload.songs;
            state.ids = state.songs.map(song => {
                return song.id;
            });
        },
        removeSong(state, payload){
            state.songs.splice(payload.id,1);
            state.ids = state.songs.map(song => {
                return song.id;
            });
            if(payload.id < payload.songNum){
                state.songNum = payload.songNum - 1;
            }else{
                state.songNum = payload.songNum;
            }
        },
        addSong(state, payload) {
            state.songs = state.songs.concat(payload.songs);
            state.songNum = state.songs.length - 1;
            state.ids = state.songs.map(song => {
                return song.id
            });
        },
        getLyric(state, payload) {
            state.lyric = getLyric(payload.lyric);
        },
        changeSong(state, payload) {
            state.songNum = payload.num;
        },
        getCurrentTime(state, currentTime) {
            state.currentTime = currentTime.currentTime;
        },
        getDuration(state, duration) {
            state.duration = duration.duration;
        },
        getAudio(state, audio){
            state.audio = audio.audio;
        },
        getIsPlay(state, isPlay){
            state.isPlay = isPlay.isPlay;
        },
        updateUpTime(state, upTime){
            state.upTime = upTime.upTime;
        }
    },
    actions: {
        async login(context, payload) {
            try {
                const ret = await get('/count?user=', 8080, payload.user);
                if(ret != 0) {
                    context.commit("login", payload);
                    Vue.prototype.$success("Generating recommendation...");
                    const ret = await get('/recommend?user=', 8080, payload.user);
                    context.commit("updateRecommendData", { recommend: ret});
                    Vue.prototype.$success("Welcome back! " + payload.user);
                }
            } catch(err) {
                Vue.prototype.$error(err);
                payload.loading.close();
                return;
            }
            payload.loading.close();
            const ret = await get('/history?user=', 8080, payload.user);
            context.commit("updateHistoryData", { history: ret});
        },
        async getSongs(context, payload) {
            const res = await get('/song/detail?ids=', PORT, payload.ids);
            if (res.code === 200) {
                context.commit('getSongs',
                    {
                        songs: res.songs
                    });
            }
        },
        async addSong(context, payload) {
            const songLocate = this.state.ids.indexOf(payload.id);
            if (songLocate < 0) {
                const res = await get('/song/detail?ids=', PORT, payload.id);
                if (res.code === 200) {
                    context.commit('addSong',
                        {
                            songs: res.songs
                        });
                }
            } else {
                context.commit('changeSong',
                    {
                        num: songLocate
                    });
            }
        },
        async getLyric(context, payload) {
            const res = await get('/lyric?id=', PORT, payload.id);
            if (res.code === 200) {
                if (Object.prototype.hasOwnProperty.call(res, 'uncollected') || Object.prototype.hasOwnProperty.call(res, 'nolyric')) {
                    context.commit('getLyric',
                        {
                            lyric: '[00:00.000] 暂无歌词\n'
                        });
                } else {
                    context.commit('getLyric',
                        {
                            lyric: res.lrc.lyric
                        });
                }
            }
        },
        getCurrentTime(context, currentTime) {
            context.commit('getCurrentTime',
                {
                    currentTime: currentTime.currentTime
                });
        },
        getDuration(context, duration) {
            context.commit('getDuration', {
                duration: duration.duration
            });
        },
        getAudio(context, audio){
            context.commit('getAudio', {
                audio: audio.audio
            });
        },
        getIsPlay(context, isPlay){
            context.commit('getIsPlay', {
                isPlay: isPlay.isPlay
            });
        },
        updateUpTime(context, upTime){
            context.commit('updateUpTime',{
                upTime: upTime.upTime
            });
        }
    }
})