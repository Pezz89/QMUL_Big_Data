#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
import pdb


def main():
    # Open final output generated from hadoop
    with open("../FinalOutput.txt") as data:
        # Store each line as a string in a list
        lines = data.readlines()
        # Get the highest index of tweets
        labels = [x.split('\t')[0] for x in lines]

        # Create labels for each index to show each group's range

        # Plot data...
        x = np.arange(len(labels))
        y = np.array([int(z.split('\t')[1]) for z in lines])
        markerline, stemlines, baseline = plt.stem(x, y, '-')
        plt.xticks(x, labels, rotation='vertical')
        xmin,xmax = plt.xlim()
        xbuff = 0.025*(xmax-xmin)
        plt.xlim(xmin-xbuff,xmax+xbuff)
        plt.setp(stemlines, 'color', 'b')
        plt.yscale("log", nonposy='clip')
        plt.grid(True)
        fig = plt.gcf()
        fig.subplots_adjust(bottom=0.23)
        plt.show()


if __name__ == "__main__":
    main()
