########################################################################################################################
# This module is part of the Wikipedia Music Tree Project.                                                             #
#                                                                                                                      #
# This module should be run once the matrix built by the build_matrix_from_raw_data.py script has been created.        #
#                                                                                                                      #
# Based on that matrix, which stores the mentions to other artists contained in the Wikipedia articles of musical      #
# artists, this module will create a CSV file that can be fed into Gephi for the generation of a graph. Considering    #
# a specified root node representing an artist (say, The Beatles), this graph will show the mentions to that Wikipedia #
# article contained in the articles of other artists; the mentions to the Wikipedia articles of these other artists    #
# contained in the articles of other artists and so forth.                                                             #
#                                                                                                                      #
# There are three parameters that need to be informed:                                                                 #
# 1- root_node - The name of the artist that will be the root node.                                                    #
#                                                                                                                      #
# 2- depth - If equal to 1, the generated CSV will only include the root artist and artists whose articles mention it. #
# If equal to 2, the generated CSV will consider the root artist, the artists artists whose articles mention it,       #
# and the artists whose articles mention these other artists. And so forth. In other words, the depth parameter        #
# determines how many steps away an artist must be from the root artist in order for it to be included in the CSV.     #
#                                                                                                                      #
# 3- relative_path_matrix - Relative path to the matrix generated by the build_matrix_from_raw_data.py script.         #
#                                                                                                                      #
# 4- relative_path_graph_csv - Relative path where the resulting CSV will be saved.                                    #
#                                                                                                                      #
# The generated CSV will be organized in a way that makes it ready to be processed by the Gephi software, which will   #
# allow the resulting graph to be visualized properly. The columns it will contain are.                                #
#                                                                                                                      #
# Source (artist whose article contains the mention), Target (artist that is mentioned by the source's article),       #
# Weight (number of times Target is mentioned in Source's article).                                                    #
#                                                                                                                      #
# Note that root_node needs to have the name of the artist exactly as it is on the title of its Wikipedia article. For #
# example, the title of the article of The Beatles is simply "The Beatles". But many artists, like The Replacements,   #
# have article titles that include the disambiguation term; in that case, root_node needs to include that part,        #
# so root_node should be "The Replacements (band)".                                                                    #
########################################################################################################################

########################################################################################################################
####################################################### Imports ########################################################
########################################################################################################################

from absl import  app, flags
from absl.flags import FLAGS
import numpy as np
import os
import pandas as pd
import re

########################################################################################################################
##################################################### Parameters #######################################################
########################################################################################################################

flags.DEFINE_string('root_node', 'Bob Dylan', 'Musical artist that will be the root of the graph.')
flags.DEFINE_integer('depth', 1, 'Defines how distant an artist must be from the root in order for them to appear in the graph. Must be 1 or more.')
flags.DEFINE_string('relative_path_matrix', '../data/resulting_mention_matrix/matrix.csv', 'Relative path to the input matrix.')
flags.DEFINE_string('relative_path_graph_csv', '../data/graph_data/', 'Relative path to the input matrix.')

########################################################################################################################
######################################################## Main ##########################################################
########################################################################################################################

def main(_argv):
    """
    This function represents the module's main flow of execution. Here, the matrix generated by the build_matrix_from_raw_data.py
    script will be read and the parameters root_node and depth will be used to build a CSV containing multiple artists and the mentions
    to them that are made in the articles of other artists.
    """

    # Obtaining the specified parameters.
    root_node = FLAGS.root_node
    depth = FLAGS.depth
    relative_path_matrix = FLAGS.relative_path_matrix
    relative_path_graph_csv  = FLAGS.relative_path_graph_csv

    # Converting relative paths to absolute ones.
    file_dir = os.path.dirname(os.path.abspath(__file__))
    path_matrix = os.path.join(file_dir, relative_path_matrix)
    path_graph_csv = os.path.join(file_dir, relative_path_graph_csv)

    # Creating an empty data frame that will be filled up throughout this code.
    df_csv = pd.DataFrame(columns=['Source', 'Target', 'Weight'])

    # Reading the matrix generated by the build_matrix_from_raw_data.py script. It has all artists whose Wikipedia articles
    # we are using and the mentions to other artists contained in those articles.
    df_matrix = pd.read_csv(path_matrix, sep=',', encoding='UTF-8')

    # Filling up the NaN values with empty strings. Some artists may have articles that do not mention any other artists, which makes column
    # MENTIONED_ARTISTS be a potential spot for NaN values. Since values of that kind would produce an error in a search we are going to do
    # down the line, here they are replaced with empty spaces.
    df_matrix = df_matrix.fillna('')
    
    # Here, we get the list of all artists contained in the matrix.
    artists_list = list(df_matrix['ARTIST_NAME'])

    # We will use that list as the index for the matrix. It will make finding artists we want much easier and faster.
    df_matrix.index = artists_list

    # Here, we create two sets that will be essential to our processing. The first will contain what we are calling "selected artists",
    # which are the artists whose articles will be considered when building the resulting CSV. The second list will contain what we are
    # calling "mentioned_artists", who are the artists mentioned in the articles of the "selected artists". These lists are sets because
    # we do not want repetitions to occur. If an artist is already in the list, they do not have to be added again.
    selected_artists_list = set()
    related_artists_list = set()

    # The first selected artist is, of course, the one specified by the user as the root of the graph.
    selected_artists_list.add(root_node)

    # Now we start a loop limited by the specified depth. Beginning from the artist specified as the root, we will identify who are the "selected
    # artists" (that is, those that will be shown in the graph).
    for i in range(0, depth):

        # Obtaining the artists that are related to the selected artists. For each artist in the selected_artists_list, we look for their name
        # in the MENTIONED_ARTISTS column of the matrix. If the name of the selected artist appears in the column, that means the artist in ARTIST_NAME
        # has a Wikipedia article that mentions the selected artist. Consequently, the artists returned by the filter are added to the related_artists_list,
        # since they are related to our selected artist. In the first iteration of the loop, the selected_artists_list will contain only the artist
        # specified by the user as the node. As iterations go on, in case depth is not equal to 1, the selected_artists_list will grow to include the
        # related_artists of the previous loop, therefore expanding the graph step by step.
        for selected_artist in selected_artists_list:
            artists_that_mention_selected_artist = df_matrix[df_matrix['MENTIONED_ARTISTS'].str.contains(re.escape(selected_artist))]['ARTIST_NAME']
            related_artists_list = related_artists_list.union(set(artists_that_mention_selected_artist))

        # Related artists are added to the selected_artists_list. That way, the artists that were included in the related_artists_list in this iteration 
        # will be in the selected_artists_list during the next one.
        selected_artists_list = selected_artists_list.union(set(related_artists_list))

    # Now that we have all of our selected and related artists, we will create the CSV that will register the links between them. We begin by iterating
    # the list of selected artists.
    for selected_artist in selected_artists_list:

        # This is just an extra check to avoid errors in the processing. One of the first steps we performed was using the variable
        # artists_list as the index for df_matrix. Since we will use that index now to look for the artist we want, here we make sure that
        # the artist we are looking for is in the index.
        if (selected_artist in artists_list):

            # Our list of related artists will contain all of those who appear in the MENTIONED_ARTISTS column of our current selected artist.
            related_artists_list = df_matrix.loc[selected_artist, 'MENTIONED_ARTISTS']

            # MENTIONED_ARTISTS is a string/list of ARTIST_NAME:NUMBER_OF_MENTIONS tuples separated by ";". Here,
            # we turn that string into an actual list by splitting via ";".
            related_artists_list = str(related_artists_list).split(";")

            # Now we have one selected artist and its list of MENTIONED_ARTISTS. Let's sweep that list one by one and register their connections
            # in the CSV.
            for related_artist in related_artists_list:
                
                # Each related_artist is a ARTIST_NAME:NUMBER_OF_MENTIONS tuple. We split by ":" in order to obtain just the name
                # and check if that artist should appear on the graph based on the specified depth. That check is done via the selected_artists_list
                # built in the previous loop. If the artist is in the list, he will be in the CSV and, therefore, in the graph.
                if (related_artist.rsplit(":")[0] in selected_artists_list):

                    # Here we create a new record. Source will be the selected_artist. Target will be the related_artist. And Weight will be the
                    # number of times Target is mentioned in Source's Wikipedia article.
                    new_csv_record = pd.DataFrame(np.array([[selected_artist, related_artist.rsplit(":")[0], related_artist.rsplit(":")[1]]]), columns=['Source', 'Target', 'Weight'])

                    # Adding line to the dataframe that will be turned into a CSV later on.
                    df_csv = df_csv.append(new_csv_record)

    # After building the CSV, we save it to the specified folder.
    # The name of the CSV will be root_node-toRoot-depthX.csv
    df_csv.to_csv(os.path.join(path_graph_csv, root_node.replace(" ", "") + "-toRoot-" + "depth" + str(depth) + ".csv"), sep=',', index=False, encoding='UTF-8')

if __name__ == '__main__':
    try:
        app.run(main)
    except SystemExit:
        pass