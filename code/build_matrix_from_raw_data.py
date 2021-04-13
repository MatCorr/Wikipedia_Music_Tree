########################################################################################################################
# This module is part of the Wikipedia Music Tree Project.                                                             #
#                                                                                                                      #
# The goal of this module is to create, from raw data extracted straight from Wikipedia, a data structure that maps    #
# the links between the Wikipedia pages of musical groups and singers.                                                 #
#                                                                                                                      #
# The transformation process done in this module involves the following steps:                                         #
#                                                                                                                      #
# 1- Converting the XML files containing the Wikipedia pages of musical groupss and singers into CSV files while       #       
# keeping only relevant information.                                                                                   #
#                                                                                                                      #
# 2- Initializing the table that will register the relationship between the downloaded Wikipedia pages. This will be   #
# done by selecting all unique musical groups and singers from the downloaded database and assigning them to ARTIST    #
# column of the table.                                                                                                 #
#                                                                                                                      #
# 3- Filling the table with information about the links between the pages. This will be done by, for every artist      #
# (row), the creation of a column called MENTIONED_ARTISTS that will store the names of the singers/musical grops      #
# mentioned in the Wikipedia article of that artist (row).                                                             #
#                                                                                                                      #
# 4- Cleaning the table to get rid of any unnecessary information originated from the raw Wikipedia files.             #                                                      
#                                                                                                                      #
# The result will be a CSV table with a list of artists and the singers/musical groups mentioned in the respective     #
# Wikipedia article as well as the number of mentions. For instance:                                                   #
#                                                                                                                      #
#  -----------------------------------------------------------------------                                             #
# |   ARTIST      |                   MENTIONED_ARTISTS                   |                                            #
# | THE BEATLES   | THE ROLLING STONES:5; THE KINKS:2; JOHN LENNON:20     |                                            #
# -------------------------------------------------------------------------                                            # 
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
import xml.etree.ElementTree as ET

########################################################################################################################
##################################################### Parameters #######################################################
########################################################################################################################

flags.DEFINE_string('relative_path_xml', '../data/xml/', 'Relative path containing the raw XML files.')
flags.DEFINE_string('relative_path_csv', '../data/csv/', 'Relative path where the created CSVs will be saved.')
flags.DEFINE_string('relative_path_matrix', '../data/resulting_mention_matrix/', 'Relative path the matrix will be saved.')

########################################################################################################################
###################################################### Functions #######################################################
########################################################################################################################

def convert_xml_to_csv(path_xml, path_csv):
    """
    This function will read the raw XML files (which contain the Wikipedia articles for musical groups and singers) 
    and convert them into a clean CSV file.
    """

    # Obtaining the full path of all XML files of singers and musical groups contained within the specified path.
    singer_files = os.listdir(os.path.join(path_xml, 'singers'))
    singer_files = [os.path.join(path_xml, 'singers', xml_file) for xml_file in singer_files]

    musical_groups_files = os.listdir(os.path.join(path_xml, 'musical_groups'))
    musical_groups_files = [os.path.join(path_xml, 'musical_groups', xml_file) for xml_file in musical_groups_files]

    all_files = singer_files + musical_groups_files

    # Here we obtain the total number of files in order to keep track of the processing and create a variable to count 
    # how many files have been processed.
    total_files = len(all_files)
    processed_count = 1

    # Given the way Wikipedia categorizes singers, there is bound to be some repetition in the XMLs we have downloaded.
    # That's because while bands can be obtained according to the year they formed, artists are organized by the centuries
    # in which they were active. Since we are working with the 20th and 21st centuries, there is bound to be some crossover
    # with artists who were active during both. For that reason, we will create the list below to keep track of artists
    # that have alread been processed. That way, there will be no repetition in the CSVs generated.
    processed_artists = []

    # We now go through all XMLs. For each one, we obtain the root of the file and iterate through the "page" tag. 
    # Since each XML contains singers or musical groups from a certain time period, as categorized by Wikipedia,
    # iterating through the "page" tag means reading the page of each singer or musical group one by one.
    for xml_file in all_files:

        # Just a friendly processing message.
        print('Processing file ' + str(processed_count) + ' out of ' + str(total_files) + '.')

        # Obtaining the file's root.
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Every XML file we read will generate a corresponding CSV containing only the desired information: the artist
        # name and the text of its Wikipedia page. Here, the data frame that will store this resulting CSV is created.
        csv_file = pd.DataFrame(columns=('ARTIST_NAME', 'WIKIPEDIA_TEXT'))

        # Iterating through all Wikipedia pages contained in the CSV.
        for page in root.iter('page'):
            
            # The name of the artist is the title of the page.
            artist_name = page.find('title').text
            artist_name = artist_name.strip()

            # If the artist has not been processed already, they will now. With this condition,
            # we are also eliminating artists that have the substring 'Category:' in their name.
            # That is because Wikipedia has subcategory files that are returned as pages when
            # certain categories are downloaded.
            if artist_name not in processed_artists and 'Category:' not in artist_name:

                # Now we get the page's text.
                wikipedia_text = page.find('text').text

                # The only part of the text we do not want to work with is the "References" section. Therefore, here
                # we throw it out.
                wikipedia_text = re.sub('<ref>[^>]+</ref>', '', wikipedia_text)
                wikipedia_text = re.sub('<ref[^>]+>[^>]+</ref>', '', wikipedia_text)
                wikipedia_text = wikipedia_text.split('==References==', 1)[0]

                # In addition, we also get rid of line breaks.
                wikipedia_text = ''.join(wikipedia_text.splitlines())

                # Adding new line to resuting CSV file. The line, naturally, will contain the artist's name and the text
                # of its corresponding Wikipedia page
                csv_file.loc[len(csv_file)] = [artist_name, wikipedia_text]

                # Adding artist to processed list.
                processed_artists.append(artist_name)

        # Obtaining the basename of the XML file without the extension. The name of the generated CSV file will the same.
        csv_name = os.path.basename(xml_file).split('.')[0] + '.csv'

        # Saving the file.
        csv_file.to_csv(os.path.join(path_csv, csv_name), sep=',', index=False, encoding='UTF8')

        # Adding to the processed count.
        processed_count += 1


def initialize_matrix(path_csv, path_matrix):
    """
    This function will read all CSVs generated in the convert_xml_to_csv function and initialize the matrix of mentions
    according to them.
    """

    # Obtaining all CSV files.
    csv_files = os.listdir(path_csv)
    csv_files = [os.path.join(path_csv, csv_file) for csv_file in csv_files]

    # Here we obtain the total number of files in order to keep track of the processing and create a variable to count 
    # how many files have been processed.
    total_files = len(csv_files)
    processed_count = 1

    # Creating the list that will store the names of all artists.
    artists = []

    # Reading all CSV files in order to obtain the names of all artists in the database.
    for csv_file in csv_files:

        # Just a friendly processing message.
        print('Processing file ' + str(processed_count) + ' out of ' + str(total_files) + '.')

        # The CSV file is read.
        df_artists = pd.read_csv(csv_file, sep=',', encoding='UTF-8')

        # The artists it contains are obtained and concatenated to the list.
        artists = artists + df_artists['ARTIST_NAME'].tolist()

        # Adding to the processed count.
        processed_count += 1

    # We sort the list of obtained artists.
    artists = sorted(artists)

    # Creating a data frame containing the ARTIST_NAME column, which will be filled up by the artists we have identified in this step,
    # and the MENTIONED_ARTISTS columns, which will be filled in the next step.
    df_matrix = pd.DataFrame(columns=['ARTIST_NAME', 'MENTIONED_ARTISTS'], index=artists)

    # Filling up the ARTIST_NAME column.
    df_matrix['ARTIST_NAME'] = artists

    # Saving the matrix.
    df_matrix.to_csv(os.path.join(path_matrix, 'matrix.csv'), sep=',', header=True, index=False, encoding='UTF-8')


def build_matrix(path_csv, path_matrix):
    """
    This is the main function of this module. Here, we will fill up the matrix of Wikipedia mentions that was initialized in the
    initialize_matrix function. Each artist will have the text of its corresponding Wikipedia article swept in search of mentions
    to other artists in the list. The result will be, for each row (artist) of the matrix, a list separated by semicolons containing
    the name of the artist that was mentioned and the number of times they appeared in the article.
    """

    # Initializing variables that will be used to count the mentions that the article of a given artist has to other artists.
    # There a few counters here, and the difference between them will be explained further down the code as they are used.
    number_of_mentions_via_hyperlink = 0
    number_of_mentions = 0

    # Obtaining all CSV files. They contain the texts of the Wikipedia articles for each artist.
    csv_files = os.listdir(path_csv)
    csv_files = [os.path.join(path_csv, csv_file) for csv_file in csv_files]

    # Obtaining all unique artist names. These will come from the matrix initialized in the initialize_matrix function.
    df_matrix = pd.read_csv(os.path.join(path_matrix, 'matrix.csv'), sep=',', encoding='UTF-8')
    unique_artists_list = list(df_matrix['ARTIST_NAME'])

    # The index of the resulting matrix will be the names of the artists themselves.
    df_matrix.index = unique_artists_list

    # The matrix will be made up of lists of strings: the ARTIST_NAME column and the MENTIONED_ARTISTS column.
    df_matrix = df_matrix.astype(np.str)

    # Here we obtain the total number of files in order to keep track of the processing and create a variable to count 
    # how many files have been processed.
    total_files = len(csv_files)
    processed_count = 1

    # Here is the core of the method. The CSV files will be read one by one. All artists inside each CSV file will be processed.
    # For each artist, its Wikipedia text will be analyzed for mentions to other artists. Little by little, the tuple made up of
    # ARTIST_NAME:NUMBER_OF_METIONS;ARTIST_NAME:NUMBER_OF_METIONS;etc will be created. In the end, this will be attributed to the
    # MENTIONED_ARTISTS column.
    for csv_file in csv_files:

        # Just a friendly processing message.
        print('Processing file ' + str(processed_count) + ' out of ' + str(total_files) + '.')

        # Reading the CSV file that contains the ARTIST_NAME its WIKIPEDIA_TEXT.
        df_artists_csv = pd.read_csv(csv_file, sep=',', encoding='UTF-8')

        # We will iterate over all artists in the CSV file.
        for idx, row in df_artists_csv.iterrows():
            
            # Here, we create the string that will store the ARTIST_NAME:NUMBER_OF_METIONS tuple.
            mentioned_artists_string = ""

            # We obtain the name of the artist and its Wikipedia text.
            artist_name = row['ARTIST_NAME']
            wikipedia_text = row['WIKIPEDIA_TEXT']

            # This is the search loop. Just above, we obtained an artist name and its corresponding Wikipedia text from the CSV file.
            # Here, we will sweep the list of unique artists and search for each one of them in the Wikipedia text we are working with.
            for mentioned_artist in unique_artists_list:

                # In order to verify if the artist is mentioned in the text, we have to look for a link to that artist's page. By default,
                # if one Wikipedia article mentions something (a band, an artist, a city, an object, etc) that has another Wikipedia article,
                # only the first mention is guaranteed to have a link. For example, in the Wikipedia article of The Rolling Stones, The Beatles 
                # are mentioned multiple times, but only some appearances of their name will feature links.
                # 
                # In the code below, we are looking exclusively for mentions in that linked format. The search is different because, behind the curtains
                # in Wikipedia code, the link refers to the artist through their "raw" name (the one used for eventual disambiguations). The band Boston, 
                # for example, due to the disambiguation with the city of Boston, will show up as "Boston (band)".
                #
                # The verification of the existence of a link containing the "raw" name of the artist is also of the utmost importance to avoid counting
                # "fake" mentions. Going back to the Boston example, if the Wikipedia text by an artist has zero links to the page of "Boston (band)",
                # that means all appearences of the word "Boston" in the text likely refer to the city and should not be counted and should be ignored.

                # Here, we look for the link (which is between a pair of brackets) to the page of that artist in the Wikipedia text we are currently
                # analyzing.
                number_of_mentions_via_hyperlink = wikipedia_text.count("[[" + str(mentioned_artist))

                # Sometimes, to make matters a little trickier, the link does not actually contain the band's name in its 100% "raw" state. There are
                # cases, observed especially with bands starting with the article "The", where the link fill feature the first "t" in a non-capitalized
                # format. In order to capture these mentions as well, the next steps are done.

                # First, we split the name of the artist we are looking for in its first empty space.
                link_non_capitalized_double_check = mentioned_artist.split(' ', 1)

                # If the first part of that split name is "The", we should look for mentions with "the" as well.
                if (link_non_capitalized_double_check[0] == 'The'):
                    
                    # Here, we transform the first "The" into "the".
                    link_non_capitalized_double_check[0] = link_non_capitalized_double_check[0].lower()
                    link_non_capitalized_double_check = ' '.join(link_non_capitalized_double_check)

                    # And now we check for other mentions via link.
                    number_of_mentions_via_hyperlink = wikipedia_text.count("[[" + str(link_non_capitalized_double_check)) + number_of_mentions_via_hyperlink

                # Now we need to check for mentions without a link. Before doing that, we need to remove the part of the artist's name
                # used by Wikipedia for disambiguations. Below, we remove all text between parentheses, with the parentheses included.
                # That way, "Boston (band)" becomes "Boston".
                clean_mentioned_artist = str(re.sub(r'\([^)]*\)', '', str(mentioned_artist)))

                # Now we count the mentions to the artist's name without a link.
                number_of_mentions = wikipedia_text.count(clean_mentioned_artist)

                # Again, it has been observed that some bands starting with "The" are sometimes mentioned with that article not capitalized.
                # So, once more, for bands that start with "The", we put that first article into a lowercase format and redo the search. 
                non_capitalized_double_check = clean_mentioned_artist.split(' ', 1)

                if (non_capitalized_double_check[0] == 'The'):

                    non_capitalized_double_check[0] = non_capitalized_double_check[0].lower()
                    non_capitalized_double_check = ' '.join(non_capitalized_double_check)
                    number_of_mentions = wikipedia_text.count(non_capitalized_double_check) + number_of_mentions

                # We are done counting. Now we add the band and the count to the tuple that will eventually be
                # put in the MENTIONED_ARTISTS column. But first, before adding the artist to the string, we need 
                # to check a few things.
                #
                # 1- The artist we are searching for in the text cannot be the artist to which the Wikipedia article
                # itself belongs. We are not looking for references to The Beatles in the Wikipedia article of
                # The Beatles, after all. This check could have been put before the loop to avoid the searches,
                # but I thought the logic would be much clearer if left here. Plus, the processing that could have 
                # been avoided is not that significant.
                #
                # 2- There needs to exist at least one mention to the artist via hyperlink. This check avoids the count of
                # mentions to "Boston", or other bands/artists whose names are common, when there is no
                # link to the band's page. In that case, all mentions to Boston probably refer to the city,
                # so we can ignore it altogether.
                #
                # Note: There is still the risk of getting the wrong counts when the name of the band is commonplace.
                # For example, consider the band Boston again. If there is a link to the band's page in the article,
                # all subsequent references to Boston in the text will be considered as being to the band, even if that
                # is not the case. The solution to that problem is currently not implemented.
                if artist_name != str(mentioned_artist) and number_of_mentions_via_hyperlink > 0:

                    # Adding the ARTIST_NAME:NUMBER_OF_METIONS information to the string.
                    #
                    # Note that we are not considering the number_of_mentions_via_hyperlink when creating the string.
                    # That hyperlink total is only used in the check above. We do so because the total contained in
                    # number_of_mentions already includes the mentions via hyperlink. Hyperlinks in Wikipedia text are
                    # formed with double brackets, as in "[[The Beatles]]". As such, when we count instances of
                    # "The Beatles" or "the Beatles" in the text, we are already considering hyperlink mentions.
                    mentioned_artists_string = mentioned_artists_string + str(mentioned_artist) + ":" + str(number_of_mentions) + ";"

            # Once we are done counting for all artists in a given Wikipedia article, we add mentioned_artists_string
            # to the resulting matrix. Note that we remove the final character of mentioned_artists_string, because it will
            # always be a ";", and we want to trim it out to keep things neat.
            df_matrix.loc[artist_name, 'MENTIONED_ARTISTS'] = mentioned_artists_string[:-1]

        # Adding to the processed count.
        processed_count += 1

    # At last, we save the matrix.
    df_matrix.to_csv(os.path.join(path_matrix, 'matrix.csv'), sep=',', index=False, encoding='UTF-8')


########################################################################################################################
######################################################### Main #########################################################
########################################################################################################################


def main(_argv):
    """
    This function represents the module's main flow of execution. It will be responsible for calling the remaining functions in
    the apppropriate order to generate the results.
    """

    # Obtaining the specified parameters.
    relative_path_xml = FLAGS.relative_path_xml
    relative_path_csv = FLAGS.relative_path_csv
    relative_path_matrix = FLAGS.relative_path_matrix

    # Converting relative paths to absolute ones.
    file_dir = os.path.dirname(os.path.abspath(__file__))
    path_xml = os.path.join(file_dir, relative_path_xml)
    path_csv = os.path.join(file_dir, relative_path_csv)
    path_matrix = os.path.join(file_dir, relative_path_matrix)

    # Executing the first step. That is, the conversion of the XML files downloaded straight from Wikipedia into cleaner CSV files that
    # only contain the information we want for each musical artist: their name and the text of the page.
    print('Step 1 - Converting XMLs to CSVs - Starting')
    convert_xml_to_csv(path_xml, path_csv)
    print('Step 1 - Converting XMLs to CSVs - Finished')

    # Executing the second step. That is, the initialization of the matrix of mentions with the names of all artists contained in our database.
    print('Step 2 - Initializing Matrix of Mentions - Starting')
    initialize_matrix(path_csv, path_matrix)
    print('Step 2 - Initializing Matrix of Mentions - Finished')

    # Executing the third step. That is, the creation of the matrix that will contain the name of the artist and the artists that its Wikipedia article
    # refer to, alongside the count of the number of times each artist is mentioned. 
    print('Step 3 - Filling Up Matrix of Mentions - Starting')
    build_matrix(path_csv, path_matrix)
    print('Step 3 - Filling Up Matrix of Mentions - Finished')

if __name__ == '__main__':
    try:
        app.run(main)
    except SystemExit:
        pass