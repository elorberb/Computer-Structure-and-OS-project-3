from threading import Thread
import time
import sys

global found
global location
global chars

#calculate the length of the searched string with delta
def length_with_delta(string_to_search, delta):
    if delta > 0:
        delta -= 1
    else:
        delta = abs(delta)
    total = 0
    total += len(string_to_search)
    total += (len(string_to_search) - 1) * abs(delta)
    return total
#search function
def search_string(text, string_to_search, delta, start_index):
    global found
    global location
    global chars
    #delta = -1 case - building char dict
    if delta == -1:
        for i in range(len(text)):
            c = text[i]
            if c in string_to_search:
                if c not in chars.keys():
                    chars[c] = []
                loc_in_full_text = i+start_index
                chars[c].append(loc_in_full_text)
    #delta  != -1 case - searching the string
    else:
        len_with_delta = length_with_delta(string_to_search, delta)
        for i in range(len(text)):
            char = text[i]
            if found:
                return
            if char == string_to_search[0]:
                res = ''
                length = i + len_with_delta
                for j in range(i, length, delta):
                    if j < len(text):
                        res += text[j]
                if res == string_to_search:
                    location = i + start_index
                    found = True
#process text data
def process_split_text(textfile,nThreads):
    file = open(textfile, 'r', encoding='utf-8-sig')
    lines_string = ""
    for line in file:
        line = line.strip('\n') #removing \n
        lines_string += line

    #calculate length of text for each thread
    length_lines = len(lines_string)
    thread_slice = length_lines // nThreads
    remain_lines = length_lines % nThreads

    return lines_string, thread_slice, remain_lines
#create and start threads
def create_start_threads(nThreads,lines_string,thread_slice,remain_lines,len_with_delta, string_to_search, delta):
    threads = []
    for i in range(nThreads):
        start = thread_slice * i  # start index for current thread to scan
        if i != nThreads - 1:  # if last thread add the remain letters
            end = thread_slice * i + thread_slice + len_with_delta
        else:
            end = thread_slice * i + thread_slice + remain_lines
        thread_string_part = lines_string[start:end]

        # create threads
        thread = Thread(target=search_string, args=(thread_string_part, string_to_search, delta, start))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
#handle delta = -1 case
def case_negative_delta(string_to_search):
    # first char to search
    c1 = string_to_search[0]
    chars_index_array = []

    # if the char founded in dictionary keep searching
    if c1 in chars:
        # find min index of char occurrence - returned result if found
        start_min_index = min(chars[c1])
        curr_c_min_index = start_min_index
        chars_index_array.append(start_min_index)
        letters_count = 0  # counting number of times min char is smaller the the other chars
        for i in range(1, len(string_to_search)):
            c2 = string_to_search[i]
            if c2 not in chars:  # second char is not in founded chars
                return 'not found'

            sorted_c2_indexes = sorted(chars[c2])
            for c2_index in sorted_c2_indexes:
                # if c1 index is smaller then c2 index check next char in string
                if curr_c_min_index < c2_index:
                    curr_c_min_index = c2_index  # update min index to c2 index
                    letters_count += 1  # add 1 to letters count
                    chars_index_array.append(c2_index)  # add index to array of founded char indexes
                    break
        if letters_count == len(string_to_search) - 1:  # if we found the letters in string
            chars.clear()  # Clean chars dict for next test
            return chars_index_array
#DistributeSearch function
def distributedSearch(textfile, string_to_search, nThreads, delta):
    global chars
    global found
    global location
    found = False
    lines_string = ""
    chars = {}

    #input from cmd is String so it should be convert to int
    delta = int(delta)
    nThreads = int(nThreads)

    if delta >= 0:
        delta += 1

    len_with_delta = length_with_delta(string_to_search, delta) #calculate length with delta
    lines_string, thread_slice, remain_lines = process_split_text(textfile,nThreads) #process text
    create_start_threads(nThreads, lines_string, thread_slice, remain_lines, len_with_delta, string_to_search, delta)

    if delta == -1:
        return case_negative_delta(string_to_search)

    if found:
        found = False #set for next test
        return location #return the location found
    else:
        return 'not found.' #if not found
#Tests
if __name__ == '__main__':
    textfile = sys.argv[1]
    string_to_search = sys.argv[2]
    nThreads = sys.argv[3]
    delta = sys.argv[4]

    print(distributedSearch(textfile, string_to_search, nThreads, delta))




