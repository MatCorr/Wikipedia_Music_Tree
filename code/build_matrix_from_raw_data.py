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
# 2- Obtaining all artists of the database and their respective cateogories.                                           #
#                                                                                                                      #
# 3- Initializing the table that will register the relationship between the downloaded Wikipedia pages. This will be   #
# done by selecting all unique musical groups and singers from the downloaded database and assigning them to ARTIST    #
# column of the table.                                                                                                 #
#                                                                                                                      #
# 4- Filling the table with information about the links between the pages. This will be done by, for every artist      #
# (row), the creation of a column called MENTIONED_ARTISTS that will store the names of the singers/musical grops      #
# mentioned in the Wikipedia article of that artist (row).                                                             #
#                                                                                                                      #                                                  
#                                                                                                                      #
# The result will be a CSV table with a list of artists, the singers/musical groups mentioned in the respective        #
# Wikipedia article  as well as the number of mentions, and the Wikipedia category the article falls into.             #
# For instance:                                                                                                        #
#                                                                                                                      #
#  ----------------------------------------------------------------------------------------------------------------    #
# |   ARTIST      |                   MENTIONED_ARTISTS                   |             ARTIST_CATEGORY            |   #
# | THE BEATLES   | THE ROLLING STONES:5; THE KINKS:2; JOHN LENNON:20     |           musical_groups_1960          |   #
# -----------------------------------------------------------------------------------------------------------------    # 
########################################################################################################################

########################################################################################################################
####################################################### Imports ########################################################
########################################################################################################################

from absl import  app, flags
from absl.flags import FLAGS
from functools import partial
import glob
import multiprocessing as mp
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
    singer_files = [os.path.join(path_xml, 'singers', xml_file) for xml_file in singer_files if os.path.splitext(xml_file)[1] == '.xml']

    musical_groups_files = os.listdir(os.path.join(path_xml, 'musical_groups'))
    musical_groups_files = [os.path.join(path_xml, 'musical_groups', xml_file) for xml_file in musical_groups_files if os.path.splitext(xml_file)[1] == '.xml']

    all_files = singer_files + musical_groups_files

    # Here we obtain the total number of files in order to keep track of the processing and create a variable to count 
    # how many files have been processed.
    total_files = len(all_files)
    processed_count = 1

    # We now go through all XMLs. For each one, we obtain the root of the file and iterate through the "page" tag. 
    # Since each XML contains singers or musical groups from a certain time period, as categorized by Wikipedia,
    # iterating through the "page" tag means reading the page of each singer or musical group one by one.
    for xml_file in all_files:

        # Just a friendly processing message.
        print('Processing file ' + str(processed_count) + ' out of ' + str(total_files) + '.', end='\r')

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

            # With this condition, we are eliminating artists that have the substring 'Category:' in their name.
            # That is because Wikipedia has subcategory files that are returned as pages when certain categories 
            # are downloaded.
            if 'Category:' not in artist_name:

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

        # Obtaining the basename of the XML file without the extension. The name of the generated CSV file will the same.
        csv_name = os.path.basename(xml_file).split('.')[0] + '.csv'

        # Saving the file.
        csv_file.to_csv(os.path.join(path_csv, csv_name), sep=',', index=False, encoding='UTF8')

        # Adding to the processed count.
        processed_count += 1


def obtain_artists_categories(path_csv):
    """
    By reading the CSV files, this function will obtain a list of all artists in the database and their respective categories.
    """

    # Obtaining all CSV files.
    csv_files = os.listdir(path_csv)
    csv_files = [os.path.join(path_csv, csv_file) for csv_file in csv_files if os.path.splitext(csv_file)[1] == '.csv']

    # Creating the lists that will help us fill the dataframe above.
    artists = []
    categories = []

    # Here we obtain the total number of files in order to keep track of the processing and create a variable to count 
    # how many files have been processed.
    total_files = len(csv_files)
    processed_count = 1

    # Reading all CSV files in order to obtain the names of all artists in the database.
    for csv_file in csv_files:

        # Just a friendly processing message.
        print('Processing file ' + str(processed_count) + ' out of ' + str(total_files) + '.', end='\r')

        # The CSV file is read.
        df_artists = pd.read_csv(csv_file, sep=',', encoding='UTF-8')

        # The artists it contains are obtained and concatenated to the list.
        artists = artists + df_artists['ARTIST_NAME'].tolist()

        # Here, we fill the list of categories. It will have the exact same length as the list of artists, since we want
        # each item from one list to correspond to the same-index item of the other. The category list is filled with the
        # name of the CSV, which is the name of the category to which the Wikipedia article belongs.
        categories = categories + [os.path.basename(csv_file).split('.')[0] for i in range(len(df_artists['ARTIST_NAME'].tolist()))]

        # Adding to the processed count.
        processed_count += 1

    return artists, categories

def initialize_matrix(artists, categories, path_matrix):
    """
    Given two lists, one containing all artists of the database and another their respective categories, this function will
    remove duplicated artists, concatenate the categories to which their Wikipedia articles belong, and initialize
    the matrix of mentions that will be filled in the next step.
    """

    # Here we obtain the total number of artists (with repetition) in order to keep track of the processing and create a 
    # variable to count how many artists have been processed.
    total_artists = len(artists)
    processed_count = 1

    # Creating a data frame containing the ARTIST_NAME and ARTIST_CATEGORY columns, which will be filled up by the artists we have 
    # identified in this step; and the MENTIONED_ARTISTS column, which will be filled in the next step.
    df_matrix = pd.DataFrame(columns=['ARTIST_NAME', 'MENTIONED_ARTISTS', 'ARTIST_CATEGORY'])

    # Given the way Wikipedia categorizes singers, there is bound to be some repetition in our list of artists. 
    # That's because while bands can be obtained according to the year they formed, artists are organized by the centuries
    # in which they were active. Since we are working with the 20th and 21st centuries, there is going to be some crossover
    # with artists who were active during both. In case an artist belongs to two categories, we don't want him to appear twice
    # in our dataset, but have - instead - those two categores concatenated into a single column. This is where we do that.
    # We start by iterating the artists and categories lists simultaneously.
    for artist, category in zip(artists, categories):

        # Just a friendly processing message.
        print('Processing artist ' + str(processed_count) + ' out of ' + str(total_artists) + '.', end='\r')

        # Checking if artist is already in dataframe.
        if artist in df_matrix.ARTIST_NAME.values:

            # We get the index where the artist is located.
            index = df_matrix.index[df_matrix['ARTIST_NAME'] == artist][0]

            # We concatenate the current category to the existing one.
            df_matrix.loc[index, 'ARTIST_CATEGORY'] = df_matrix.loc[index, 'ARTIST_CATEGORY'] + ';' + category
        
        else:
            # If the artist is not in the dataframe, we will simply add it and its category to it.
            df_matrix = df_matrix.append({'ARTIST_NAME': artist,
                                          'MENTIONED_ARTISTS':  '',
                                          'ARTIST_CATEGORY': category}, ignore_index=True)

        # Adding to the processed count.
        processed_count += 1

    # We sort the dataframe by ARTIST_NAME.
    df_matrix = df_matrix.sort_values(by=['ARTIST_NAME'])

    # We save the initialized matrix.
    df_matrix.to_csv(os.path.join(path_matrix, 'matrix.csv'), sep=',', header=True, index=False, encoding='UTF-8')

def init_globals(counter):
    """
    This function is used to define the counter that will be shared between parallelized processes.
    """
    global processed_count
    processed_count = counter

def process_wikipedia_articles_parallel(full_artists_list, total_artists, artist_wikipedia_article):
    """
    This is the core of the matrix-building process and will be called by the build_matrix method. It has been separated
    from that function so it can be called in a parallelized way. Here, the Wikipedia article of an artist will be
    searched for mentions to all artists present in the full_artists_list. The result will be, for each Wikipedia article 
    (which corresponds to an artist) the the list of mentions it contains. This list will be presented in the form of a string: 
    ARTIST_NAME:NUMBER_OF_METIONS;ARTIST_NAME:NUMBER_OF_METIONS;etc
    """
    
    # Printing the control message that will let us know how much the processing has advanced. Since the counter is shared,
    # the process needs to get a lock on it before proceeding.
    with processed_count.get_lock():
        print('Processing artist ' + str(processed_count.value) + ' out of ' + str(total_artists) + '.', end='\r')
        processed_count.value += 1
        
    # Here, we create the string that will store the ARTIST_NAME:NUMBER_OF_METIONS tuple.
    mentioned_artists_string = ""

    # We obtain the name of the artist and its Wikipedia text.
    artist_name = artist_wikipedia_article[0]
    wikipedia_text = artist_wikipedia_article[1]

    # This is the search loop. Just above, we obtained an artist name and its corresponding Wikipedia text from the CSV file.
    # Here, we will sweep the list of unique artists and search for each one of them in the Wikipedia text we are working with.
    for mentioned_artist in full_artists_list:

        # We don't care when the article we are checking mentions the artist to which it belongs. So we only do the search when 
        # that's not the case.
        if artist_name != str(mentioned_artist):

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
            number_of_mentions_via_hyperlink = wikipedia_text.count("[[" + str(mentioned_artist) + ']]')

            # Links can also appear with an opening pair of brackets and a closing pipe "|". Those types of links are used to format the title
            # of the linked article into something slightly different.
            number_of_mentions_via_hyperlink = number_of_mentions_via_hyperlink + wikipedia_text.count("[[" + str(mentioned_artist) + '|')

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
                number_of_mentions_via_hyperlink = wikipedia_text.count("[[" + str(link_non_capitalized_double_check) + ']]') + \
                wikipedia_text.count("[[" + str(mentioned_artist) + '|') + \
                number_of_mentions_via_hyperlink

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
            # to check a one thing.
            # 
            # There needs to exist at least one mention to the artist via hyperlink. This check avoids the count of
            # mentions to "Boston", or other bands/artists whose names are common, when there is no
            # link to the band's page. In that case, all mentions to Boston probably refer to the city,
            # so we can ignore it altogether.
            #
            # Note: There is still the risk of getting the wrong counts when the name of the band is commonplace.
            # For example, consider the band Boston again. If there is a link to the band's page in the article,
            # all subsequent references to Boston in the text will be considered as being to the band, even if that
            # is not the case. The solution to that problem is currently not implemented.
            if number_of_mentions_via_hyperlink > 0:

                # Adding the ARTIST_NAME:NUMBER_OF_METIONS information to the string.
                #
                # Note that we are not considering the number_of_mentions_via_hyperlink when creating the string.
                # That hyperlink total is only used in the check above. We do so because the total contained in
                # number_of_mentions already includes the mentions via hyperlink. Hyperlinks in Wikipedia text are
                # formed with double brackets, as in "[[The Beatles]]". As such, when we count instances of
                # "The Beatles" or "the Beatles" in the text, we are already considering hyperlink mentions.
                mentioned_artists_string = mentioned_artists_string + str(mentioned_artist) + ":" + str(number_of_mentions) + ";"

    # Once we are done counting for all artists in a given Wikipedia article, we return the final result in the form of a dictionary. 
    # Note that we remove the final character of mentioned_artists_string, because it will always be a ";", and we want to trim it 
    # out to keep things neat.
    return {artist_name : mentioned_artists_string[:-1]}

def build_matrix(path_csv, path_matrix):
    """
    This is the main function of this module. Here, we will fill up the matrix of Wikipedia mentions that was initialized in the
    initialize_matrix function. Each artist will have the text of its corresponding Wikipedia article swept in search of mentions
    to other artists in the list. The result will be, for each row (artist) of the matrix, a list separated by semicolons containing
    the name of the artist that was mentioned and the number of times they appeared in the article.
    """

    # Obtaining all CSV files. They contain the texts of the Wikipedia articles for each artist.
    csv_files = os.listdir(path_csv)
    csv_files = [os.path.join(path_csv, csv_file) for csv_file in csv_files if os.path.splitext(csv_file)[1] == '.csv']

    # In this next segment, we concatenate all CSV files into one large dataframe containing all artists and Wikipedia
    # articles of the database. Since there are artists that appear more than once, we also eliminate duplicates.
    df_artists_csv = []

    for csv_file in csv_files:
        df_temporary = pd.read_csv(csv_file, sep=',', encoding='UTF-8')
        df_artists_csv.append(df_temporary)

    df_artists_csv = pd.concat(df_artists_csv)

    df_artists_csv = df_artists_csv.drop_duplicates(subset = ["ARTIST_NAME"])

    # We have the Wikipedia articles we will analyze. Now we need the matrix we will fill up with the mentions the articles
    # contain. Here, we open it and we also build a list of all artists (which are already unique) in the matrix. This will
    # be used later.
    df_matrix = pd.read_csv(os.path.join(path_matrix, 'matrix.csv'), sep=',', encoding='UTF-8')
    df_matrix_index = list(df_matrix['ARTIST_NAME'])

    # The index of the resulting matrix will be the names of the artists themselves.
    df_matrix.index = df_matrix_index

    # Converting matrix data into strings. This is done to avoid an error that was popping up.
    df_matrix = df_matrix.astype(np.str)

    # Here is the core of the method. The list of artists / Wikipedia articles will be processed by parallel processes. 
    # For each artist, its Wikipedia text will be analyzed for mentions to other artists. Little by little, 
    # the tuple made up of ARTIST_NAME:NUMBER_OF_METIONS;ARTIST_NAME:NUMBER_OF_METIONS;etc will be created. In the end, this 
    # will be attributed to the MENTIONED_ARTISTS column.

    # In order to parallelize our processing, we need to work with lists, which will then be split accross the processes.
    # We are going to work with tuples containing an artist and its Wikipedia article. So here we create these two lsits.
    artists_list = df_artists_csv['ARTIST_NAME'].tolist()
    wikipedia_article_list = df_artists_csv['WIKIPEDIA_TEXT'].tolist()

    # Now we zip them into single object.
    artist_wikipedia_article_list = zip(artists_list, wikipedia_article_list)

    # We get the list of artists again and order it. This seems repetitive, but it is begin done because ordering the list used
    # in the zip command above will mess up the pairs, since the object artists_list will have a totally different order from the
    # one of wikipedia_article_list.
    full_artists_list = df_artists_csv['ARTIST_NAME'].tolist()
    full_artists_list.sort()

    # Here we obtain the total number of artists in order to keep track of the processing and create a variable to count how many 
    # artists have been processed. The processed_count will be shared among processes, so it is incremented by all of them.
    total_artists = len(artists_list)
    processed_count = mp.Value('i', 1)

    # Since we are going to parallelize this quite heavy processing, we start by creating a pool with as many processes
    # as the available cores. We are also initializing the shared variable processed_count.
    pool = mp.Pool(processes=mp.cpu_count(), initializer=init_globals, initargs=(processed_count,))

    # Besides sending the artist_wikipedia_article_list to the parallelized function, we need to send an extra parameter
    # that will not be split: the complete list of artists that appear in our dataset, since we will have to search for all
    # of them on each Wikipedia article. For that reason, we use the partial wraper in the process_wikipedia_articles_parallel method
    # to indicate the artists_list is not to be split among the processes. We also send the total_artists variable just to print
    # messages that will let us know how much of the processing is done.
    parallelized_function = partial(process_wikipedia_articles_parallel, full_artists_list, total_artists)

    # Now we summon the paralelized function. The processing of the artist_wikipedia_article will be split.
    artist_to_mentioned_artists_list = pool.map(parallelized_function, artist_wikipedia_article_list)

    # The result of the processing above will be a list of dictionaries, each containing a key (the artist name) and a value
    # the tuple of mentions contained in the article of that artist. Here we iterate over that dictionary and add the tuple
    # of mentions to its respective line in the df_matrix.
    for artist_to_mentioned_artists in artist_to_mentioned_artists_list:
        for artist_name in artist_to_mentioned_artists:
            mentioned_artists = artist_to_mentioned_artists[artist_name]

            df_matrix.loc[artist_name, 'MENTIONED_ARTISTS'] = mentioned_artists

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

    # Executing the second step. We sweep through the whole database creating two lists: one containing all artists, and another containing
    # their respective categories.
    print('Step 2 - Obtaining Artists and Categories - Starting')
    artists, categories = obtain_artists_categories(path_csv)
    print('Step 2 - Obtaining Artists and Categories - Finished')

    # Executing the third step. That is, the initialization of the matrix of mentions with the names of all artists contained in our database.
    print('Step 3 - Initializing Matrix of Mentions - Starting')
    initialize_matrix(artists, categories, path_matrix)
    print('Step 3 - Initializing Matrix of Mentions - Finished')

    # Executing the fourth step. That is, the creation of the matrix that will contain the name of the artist and the artists that its Wikipedia article
    # refer to, alongside the count of the number of times each artist is mentioned. 
    print('Step 4 - Filling Up Matrix of Mentions - Starting')
    build_matrix(path_csv, path_matrix)
    print('Step 4 - Filling Up Matrix of Mentions - Finished')

if __name__ == '__main__':
    try:
        app.run(main)
    except SystemExit:
        pass