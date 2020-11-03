import re
import sys
import itertools as it
from pyspark import SparkConf, SparkContext


# Celem jest liczby wspólnych znajomych
# Każdy znajomy danego użytkownika ma 1 wspólnego znajomego(tego użytkownika). 
# Kombinacja par wszytskich znajomych da pary znajmych ktorzy maja jednego wspolnego znajomego ktorym jest ten użytkownik.
# U1, [U2, U3, U4] => U2-U3, U2-U4, U3-U4 - Kazda para ma 1 wspolnego znajomego U1.
# Prowadząc takie rozbicie dla wszystkich użytkowników otrzymamy listę par posiadających wspolnych znajomych.(Będą zduplikowane wartośći).
# Po pogrupowaniu i zliczeniu otrzymamy <para><ilosc_wspolnych_znajomych>

# Tylko użytkownicy ze znajomymi są brani pod uwagę - UsersWithFriendsMap
# Do utowrzenia kombinacji użyto itertools i combinations
# Finalnie otrzymamy ((U1,U2),1) => U1,U2 - para użytkowników mających 1 wspolnego znajomego.

def combineFriends(UserWithFriends):   #user_friendslist
	#from_user = UserWithFriends[0]
	FriendList = UserWithFriends[1]
	return [(pair, 1) for pair in it.combinations(FriendList, 2)]


# Następnie musimy połączyć tych znajomych ktorzy mają tę samą liczę wspolnych znajomych
# (1,{2,[8]}), (1,{2,[4]}), (1,{2,[7]})  => (1, {2,[8,4,7]})
# Używamy reduceByKey i zdefiniowanej funkcji. 
# Dla elementów z tymi samymi kluczami- dodajemy wartość spod klucza do tablicy, jesli nie ma takiego klucza, to tworzymy nową tablice z tą wartością.
def transform(accumulatedDict, currentDict):
	for key in currentDict:
		if key in accumulatedDict:
			accumulatedDict[key] += currentDict[key]
		else:
			accumulatedDict[key] = currentDict[key]
	return accumulatedDict


# Zgodnie z wymaganiami dane musza być posortowane malejąca po ilości znajomych i rosnąco w ramach grupy znajomych.
# define custom map function
# Input: (user, {cnt1: [...], cnt2: [...], ...})
# Output (user, [(cnt1, [..]), (cnt2, [.]), ..]), cnt - descending, [...] - ascending
def Sorting(row):
	FriendsForRecommendation = []
	ShuffleFriends = row[1]
	SortedKeys = sorted(ShuffleFriends, reverse=True)        # sortuje klucze malejąco 
	for cnt in SortedKeys:
	    FriendsForRecommendation.append((cnt, sorted(ShuffleFriends[cnt])))    # Dodaje do tablicy cnt= ilosc wspolnych znajomych oraz liste posortowanych rosnąco elementów z kluczem równym cnt
	return (row[0], FriendsForRecommendation)


# Zgodnie z wymaganiemi, ilosc wspolnych znajomych jest niestotna na tym etapie.
# Zmieniamy format (U1, [ (3, [U3, U4]), (2, [U6, U9, U11]) ] ) usuwając ilości i łącząc listy.
# (U1, [U3, U4, U6, U9])
def reduceAndMergeList(row):
    user = row[0]
    RecomList = row[1]
    AmountOfElements = len(RecomList)
    ReducedList = []
    for i in range(AmountOfElements):
        for j in range(len(RecomList[i][1])):
            ReducedList.append(RecomList[i][1][j]) # [i][1][j]; 1-> tylko rekomendacje, ilość nieistotna.
    return (user, ReducedList)

# Define print function
# pMap - map to print
# pList - list of keys
#     N - upto N per key
# fname - file print to
def pMap2file(pMap, pList, N, fname):
    fp = open(fname, 'w+')
    for k in pList:
        if k in pMap:
            plen = min(len(pMap[k]), N)     # N to max ilość wyswietlanych rekomendacji. jeśli wiersz ma wiecej rekomendacji to N.
            for e in pMap[k][:plen]:
                s=','.join(str(e) )
            #s = ','.join(str(e) for e in pMap[k][:plen])
            print(str(k) +"\t"+ str(s),file=fp)
        else:
            print(str(k), file=fp)
    fp.close()


