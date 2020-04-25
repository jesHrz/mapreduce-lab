import numpy as np


class KMeans:
    def __init__(self, n_clusters, max_iter=200):
        self.n_clusters = n_clusters
        self.max_iter = max_iter

    def fit(self, X):
        m, n = np.shape(X)
        assert self.n_clusters <= m
        self.__random_cluster_centers_(X)
        self.labels_ = [0 for i in range(m)]
        for iter in range(self.max_iter):
            for i in range(m):
                k = 0
                for j in range(1, self.n_clusters):
                    if np.dot(X[i] - self.cluster_centers_[j], X[i] - self.cluster_centers_[j]) < np.dot(X[i] - self.cluster_centers_[k], X[i] - self.cluster_centers_[k]):
                        k = j
                self.labels_[i] = k
            for j in range(self.n_clusters):
                sum_x = np.zeros(n)
                sum_cnt = 0
                for i in range(m):
                    if self.labels_[i] == j:
                        sum_x += X[i]
                        sum_cnt += 1
                if sum_cnt > 0:
                    self.cluster_centers_[j] = sum_x / sum_cnt
        for i in range(m):
            k = 0
            for j in range(1, self.n_clusters):
                if np.dot(X[i] - self.cluster_centers_[j], X[i] - self.cluster_centers_[j]) < np.dot(X[i] - self.cluster_centers_[k], X[i] - self.cluster_centers_[k]):
                    k = j
            self.labels_[i] = k

    def __random_cluster_centers_(self, X):
        # import random
        # ind = random.sample([i for i in range(len(X))], self.n_clusters)
        self.cluster_centers_ = np.array([X[i] for i in range(self.n_clusters)])


def load_data(file):
    feature = []
    with open(file) as f:
        for each_line in f.readlines():
            data = each_line.strip().split("\t")[1]
            data = data.split(",")
            feature.append(list(map(float, data)))
    return feature

if __name__ == "__main__":
    X = load_data("test/lab4_data/kmeans/training_data.txt")
    m, n = np.shape(X)
    kmeans = KMeans(n_clusters=16, max_iter=50)
    kmeans.fit(X)
    ans = [[] for i in range(16)]
    for i in range(len(kmeans.labels_)):
        ans[kmeans.labels_[i]].append(i)
    for i in range(16):
        ans[i].sort()
        s = list(map(str, ans[i]))
        print(str(i) + "\t" + ",".join(s))


