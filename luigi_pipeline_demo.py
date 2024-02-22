from collections import Counter
import luigi


class FileWriter(luigi.Task):
    def output(self):
        return luigi.LocalTarget("fruits.txt")
    
    def run(self):
        fruits = ["banana", "apple", "orange", "pineapple", "grapes", "grapefruit"]
        with self.output().open("w") as f:
            f.write("\n".join(fruits))


class FileProcessor(luigi.Task):
    num_letters = luigi.IntParameter()

    def requires(self):
        return FileWriter()
    
    def output(self):
        return luigi.LocalTarget("letters.txt")
    
    def run(self):
        with self.input().open("r") as input_file:
            fruits = input_file.readlines()
        fruits = [fruit.strip() for fruit in fruits]
        letters = [fruit[:self.num_letters] for fruit in fruits]
        with self.output().open("w") as f:
            f.write("\n".join(letters))


class LetterCounter(luigi.Task):
    def requires(self):
        return FileProcessor()
    
    def output(self):
        return luigi.LocalTarget("counts.txt")
    
    def run(self):
        with self.input().open("r") as input_file:
            letters = input_file.readlines()
        letters = [letter.strip() for letter in letters]
        most_common_letters = Counter(letters).most_common(20)
        with self.output().open("w") as f:
            f.write("\n".join([str(letter) for letter in most_common_letters]))


class WordAppender(luigi.Task):
    def requires(self):
        return FileProcessor()
    
    def output(self):
        return luigi.LocalTarget("new_words.txt")
    
    def run(self):
        with self.input().open("r") as input_file:
            letters = input_file.readlines()
        letters = [letter.strip() for letter in letters]
        new_words = set([letter + "anana" for letter in letters])
        new_words = sorted(new_words)
        
        with self.output().open("w") as f:
            f.write("\n".join(new_words))


class Runner(luigi.WrapperTask):
    def requires(self):
        return LetterCounter(), WordAppender()
    

if __name__ == "__main__":
    luigi.build([Runner()], local_scheduler = True)

