import findspark
from pyspark import SparkConf, SparkContext
import string


findspark.init()
conf = SparkConf().setMaster("local").setAppName("Caesar Cipher")
sc = SparkContext(conf = conf)

alphabet = 'abcdefghijklmnopqrstuvwxyz'

def difference_of_letters(letter, compareTo='e'):
    return string.ascii_lowercase.index(compareTo.lower()) - string.ascii_lowercase.index(letter.lower())

def shift(doc, distance):
    newString = ""
    for char in doc:
        # ignores shifting everything that is not a letter
        if char.lower() not in alphabet:
            newString += char
        else:
            newIndex = string.ascii_lowercase.index(char.lower()) + distance
            # loops to front of alphabet
            if newIndex > 25:
                newIndex -= 26
            # loops to front of alphabet
            if newIndex < 0:
                newIndex += 26
            newString += alphabet[newIndex]
    return newString

def most_freq_letter(charArray, index=0):
    try:
        # the most frequent character is the first element in the array
        char = charArray[index][0]
        # searches for most freq character until a letter is found
        if str(char) not in alphabet:
            return most_freq_letter(charArray, index + 1)
        return char
    except Exception as e:
        print(e)


# import file
#fileToAnalyze = sc.textFile("Encrypted-1.txt")
#fileToAnalyze = sc.textFile("Encrypted-2.txt")
fileToAnalyze = sc.textFile("Encrypted-3.txt")

chars = fileToAnalyze.flatMap(lambda line: list(line))
charMap = chars.map(lambda char: (char.lower(), 1)).reduceByKey(lambda x, y: x + y).collect()
charMap.sort(key=lambda charTuple: charTuple[1], reverse=True)
letter = most_freq_letter(charMap)

# 10 most frequent letters in english alphabet in order
mostFreqLetters = ['e', 't', 'a', 'o', 'i', 'n', 's', 'h', 'r', 'd']
mostFreqLettersIndex = 0

print("Most frequent letter: {}".format(letter))
letterFreq = mostFreqLetters[mostFreqLettersIndex]
diff = difference_of_letters(letter, letterFreq)
print("Decoding with the letter: {}. Cipher shift of {}".format(letterFreq, diff))
decodedText = shift(chars.collect(), diff)
print(decodedText)
print("")

# save result as a specified text file
with open("decrypted-3.txt".format(fileToAnalyze), 'w+') as out_file:
    out_file.writelines(decodedText)
    out_file.close()
print("File decrypted and saved as: decrypted-3.txt".format(fileToAnalyze))
