#!/usr/bin/env python
import pdb

def main():
    with open("./denonyms_unfiltered.txt") as denonyms_uf,\
    open("./RioTeams.txt") as teams,\
    open("./denonyms_filtered.txt", 'w') as output:
        teams_list = teams.readlines()
        deno_list = denonyms_uf.readlines()
        for team in teams_list:
            output_line = ""
            for deno_line in deno_list:

                if team.rstrip().capitalize() in deno_line.capitalize().split('\t'):
                    output_line = deno_line
                    output.write(output_line)
            if output_line == "":
                raise RuntimeError("Team not found: {0}".format(team))




if __name__ == "__main__":
    main()
