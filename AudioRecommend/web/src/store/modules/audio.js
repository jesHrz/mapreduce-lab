import { get } from '@/utils/api.js';
import { parseLyric } from '@/utils/tools.js';

const PORT = 3000;
const audio = {
    namespaced: true,
    state: {
        song: null,
        lyric: null,
        id: 0,
        currentTime: 0,
        duration: 0,
        audio: {},
        isPlay: false
    },
    mutations: {
        setSong(state, song) {
            state.song = song;
        },
        setLyric(state, lyric) {
            state.lyric = parseLyric(lyric);
        },
        setCurrentTime(state, currentTime) {
            state.currentTime = currentTime;
        },
        setDuration(state, duration) {
            state.duration = duration;
        },
        setAudio(state, audio) {
            // console.log(audio);
            state.audio = audio;
        },
        setIsPlay(state, play) {
            state.isPlay = play;
        }
    },
    actions: {
        async setSong(context, id) {
            const ret = await get('/song/detail?ids=', PORT, id);
            if(ret.code === 200) {
                context.commit('setSong', ret.songs[0]);
                context.commit('setIsPlay', true);
            }
        },
        async setLyric(context, id) {
            const ret = await get('/lyric?id=', PORT, id);
            if(ret.code === 200) {
                if (Object.prototype.hasOwnProperty.call(ret, 'uncollected') || Object.prototype.hasOwnProperty.call(ret, 'nolyric')) {
                    context.commit('setLyric', '[00:00.000] 暂无歌词\n');
                } else {
                    context.commit('setLyric', ret.lrc.lyric);
                }
            }
        },
        setCurrentTime(context, currentTime) {
            context.commit('setCurrentTime', currentTime);
        },
        setDuration(context, duration) {
            context.commit('setDuration', duration);
        },
        setAudio(context, audio) {
            context.commit('setAudio', audio);
        },
        setIsPlay(context, play) {
            context.commit('setIsPlay', play);
        }
    }
}

export default audio;