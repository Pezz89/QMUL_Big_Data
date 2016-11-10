#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt


def main():
    # Open final output generated from hadoop
    with open("../FinalOutput.txt") as data:
        # Store each line as a string in a list
        lines = data.readlines()
        # Get the highest index of tweets
        indexes = np.array([x.split()[0] for x in lines], dtype=int)
        max_ind = np.max(indexes)

        # Create a 2D array of zeros to fill with index-count pairs
        data = np.zeros([max_ind, 2], dtype=int)
        # Fill first column with indexes for each category (1-5, 6-10 etc...)
        data[:, 0] = np.arange(max_ind)+1

        for line in lines:
            # Split the line into it's two components
            line = line.split()
            # Get the index stored in component 1
            ind = int(line[0])-1

            # Set column two at the index provided to the value provided
            data[ind][1] = line[1]

        # Create labels for each index to show each group's range
        labels = []
        for i in xrange(max_ind):
            labels.append(str((5*i)+1)+" - "+str((5*i)+5))

        # Plot data...
        x = data[:, 0]
        y = data[:, 1]
        markerline, stemlines, baseline = plt.stem(x, y, '-')
        plt.xticks(x, labels, rotation='vertical')
        plt.setp(stemlines, 'color', 'b')
        plt.show()


if __name__ == "__main__":
    main()
