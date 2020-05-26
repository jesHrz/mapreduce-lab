import numpy as np


def load_data(file):
    feature = []
    label = []
    with open(file) as f:
        for each_line in f.readlines():
            data = list(map(int, each_line.strip().split()))
            feature.append(data[0:-1])
            label.append([data[-1]])
    return np.mat(feature), np.mat(label)


def fit(feature, label):
    n, m = np.shape(feature)
    max_x = np.max(feature)
    max_y = np.max(label)
    p = [[[0 for i in range(max_y + 1)] for i in range(max_x + 1)] for i in range(m)]
    y = [0 for i in range(max_y + 1)]
    for j in range(n):
        y[label[j, 0]] += 1
        for i in range(m):
            p[i][feature[j, i]][label[j, 0]] += 1
    for i in range(m):
        for j in range(max_x + 1):
            for k in range(max_y + 1):
                p[i][j][k] /= y[k]
    for i in range(max_y + 1):
        y[i] /= n
        print("#%d\t%.10f" % (i, y[i]))
    for i in range(m):
        for j in range(max_x + 1):
            for k in range(max_y + 1):
                if p[i][j][k] > 0:
                    print("%d,%d,%d\t%.10f" % (i, j, k, p[i][j][k]))
    return p, y


def classify(feature, prob, y):
    ans = 0
    max_p = 0
    m = np.shape(feature)[1]
    for i in range(len(y)):
        p = y[i]
        for j in range(m):
            p *= prob[j][feature[0, j]][i]
        if p > max_p:
            max_p = p
            ans = i
    return ans


if __name__ == "__main__":
    feature, label = load_data("test/lab4_data/nb/training_data.txt")
    p, y = fit(feature, label)

