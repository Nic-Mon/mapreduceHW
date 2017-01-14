###
###
# Author Info:
#     This code is modified from code originally written by Jim Blomo and Derek Kuo
##/


from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep
import itertools
import re

WORD_RE = re.compile(r"[\w']+")

class UserSimilarity(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper1_extract_user_wordlist(self,_,record):
        """Taken a record, yield <user_id, wordlist>"""
        yield [record['user_id'], WORD_RE.findall(record['text'].lower())]

    def reducer1_compile_wordset_under_user(self,user_id,wordlists):
        """get set of words under each used_id"""
        wordset = set([word for sublist in wordlists for word in sublist])
        yield [user_id, list(wordset)]

    def mapper2_aggregate_wordsets(self, user_id, wordset):
        """aggreagte user_ids and wordsets"""
        yield ['LIST', [user_id, wordset]]

    def reducer2_calculate_similarity(self,stat,user_ids_wordsets):
        def Jaccard_similarity(list1, list2):
            """Calculate JAccard Similarity"""
            union = set(list1) | set(list2)
            intersect = set(list1) & set(list2)
            return len(intersect)/float(len(union))


        # http://stackoverflow.com/questions/942543/operation-on-every-pair-of-element-in-a-list
        for (user1, wordset1), (user2, wordset2) in itertools.combinations(user_ids_wordsets, 2):
            if Jaccard_similarity(wordset1,wordset2) >= 0.5:
                yield [[user1, user2], Jaccard_similarity(wordset1,wordset2)]
            

    def steps(self):
        return [
            MRStep(mapper=self.mapper1_extract_user_wordlist, reducer=self.reducer1_compile_wordset_under_user),
            MRStep(mapper=self.mapper2_aggregate_wordsets, reducer= self.reducer2_calculate_similarity)
        ]


if __name__ == '__main__':
    UserSimilarity.run()