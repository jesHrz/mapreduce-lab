import Vue from 'vue';
import Router from 'vue-router';

Vue.use(Router)


const routes = [
    {
        path: '/',
        name: 'home',
        component: () => import('@/views/Home.vue')
    },
    {
        path: '/search',
        name: 'search',
        component: () => import('@/views/Search.vue')
    },
    {
        path: '/artist',
        name: 'artist',
        component: () => import('@/views/Artist.vue')
    }
]

const router = new Router({
    routes
})

export default router
