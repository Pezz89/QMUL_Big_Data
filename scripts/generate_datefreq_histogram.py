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
        labels = [x.split()[0] for x in lines]

        for line in lines:
            # Split the line into it's two components
            line = line.split()[1:]

        # Create labels for each index to show each group's range

        # Plot data...
        x = labels
        y = lines[:, 1]
        markerline, stemlines, baseline = plt.stem(x, y, '-')
        plt.xticks(x, labels, rotation='vertical')
        xmin,xmax = plt.xlim()
        xbuff = 0.1*(xmax-xmin)
        plt.xlim(xmin-xbuff,xmax+xbuff)
        plt.setp(stemlines, 'color', 'b')
        plt.show()


if __name__ == "__main__":
    main()
