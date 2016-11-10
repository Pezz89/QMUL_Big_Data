#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
import pdb


def main():
    with open("../FinalOutput.txt") as data:
        lines = data.readlines()
        # Get the highest index of tweets
        max_ind = int(lines[-1].split()[0])

        data = np.zeros([max_ind, 2], dtype=int)
        data[:, 0] = np.arange(max_ind)+1
        for line in lines:
            line = line.split()
            ind = int(line[0])-1
            data[ind][0], data[ind][1] = line
        #data = data[data[:,0].argsort()]

        labels = []
        for i in xrange(max_ind):
            labels.append(str((5*i)+1)+" - "+str((5*i)+5))

        x = data[:, 0]
        y = data[:, 1]
        markerline, stemlines, baseline = plt.stem(x, y, '-')
        plt.xticks(x, labels, rotation='vertical')
        #plt.setp(markerline, 'markerfacecolor', 'b')
        #plt.setp(baseline, 'color', 'r', 'linewidth', 2)
        plt.setp(stemlines, 'color', 'b')
        plt.show()


if __name__ == "__main__":
    main()
