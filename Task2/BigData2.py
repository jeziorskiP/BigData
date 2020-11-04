import re
import sys
import itertools as it
from pyspark import SparkConf, SparkContext
import modules as M

print("REKOMENDACJE\n\n")
#print("Podaj nazwę pliku z danymi (Enter: default = t.txt ): ")
fileNameInput = input("Podaj nazwę pliku z danymi (Enter: default = t.txt ): ")
if fileNameInput =="":
    DataSetFileName = "t.txt"
else:
    DataSetFileName = fileNameInput

#print("Podaj liczbę rekomendacji: (Enter: default = 10): ")
AmountOfRecInput = input("Podaj liczbę rekomendacji: (Enter: default = 10): ")
if AmountOfRecInput =="":
    AmountOfRec = int(10)
else:
    AmountOfRec = int(AmountOfRecInput)
    
print("Rekomendacje dla wszystkich użytkowników- 1\nRekomendacja dla niestandardowych uzytkowników- 2\nRekomendacja dla testowej grupy \
#użytkowników([924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]) - 3\nRekomendacja dla użytkownika nr 11 - 4")

UserInput = input("Podaj numer: ")
if UserInput == str('1'):   #All
    Choice = int(UserInput)
elif UserInput == str('2'):   #Custom
    Choice = int(UserInput)
    
    ListLen = input("Podaj ilość elementów: ")
    CustomList = []
    for i in range(int(ListLen)):
        CustomList.append( int(  input() ) )
        
    print("Twoja lista: ", CustomList)
    
    
elif UserInput == str('3'):   #List
    Choice = int(UserInput)
elif UserInput == str('4'):   #Only USER No11
    Choice = int(UserInput)
else:
    Choice = int(0)

fileNameInput = input()


print("Spark Configuration...")
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
print("SparkContext initiated ")

# Wczytywanie danych z pliku do programu
loadedData = sc.textFile(DataSetFileName)
print("Data loaded")

# loadedData to ciąg znaków, więc należy je podzielić na User i Friend
# '0  1,2,3,' => '0' '1,2,3'
splittedUserAndFriend = loadedData.map(lambda l: l.split())

print("Splitted data. User - Friends ")
print(splittedUserAndFriend)


# Lista użytkowników, dla których będą realizowane rekomendacje.
# wszyscy użytkownicy 
OnlyUsers  = splittedUserAndFriend.map(lambda x: x[0])
usersCollection = OnlyUsers.collect()
AllUsers = map(int, usersCollection)

# Posortowna lista użytkowników - rosnąco
AllUsersSortedList = sorted(AllUsers)

print("OnlyUsers")
sample = OnlyUsers.take(10)
print(sample)


# Filtrujemy te krotki, ktore skłądają się z dwóch elementów (Użytkownik + Znajomi). Użytkownicy bez znajomych są bezużyteczni.
FiltredUsersWithFriends = splittedUserAndFriend.filter(lambda e: len(e) == 2)

print("FiltredUsersWithFriends")
print("DONE!")

# FiltredUsersWithFriends skłąda się z ('U', 'f1,f2,..'). 
# Rozdzielamy znajomych na pojedyncze elementy (int) - separator ','- i przechowujemy w tablicy - dokładniej w map(wydajniejsza)
# Otrzymujemy Użytkownik - mapa znajomych(int), posortowanych rosnąco
UsersWithFriendsMap = FiltredUsersWithFriends.map(lambda row: (int(row[0]), map(int, sorted(  row[1].split(',') ) )))

print("UsersWithFriendsMap")
sam = UsersWithFriendsMap.take(5)
print(sam)

# Cel to znalezienie liczby wspólnych znajomych
# Każdy znajomy danego użytkownika ma 1 wspólnego znajomego (tego użytkownika) z pozostałymi. 
# Kombinacja par wszytskich znajomych da pary znajomych ktorzy maja jednego wspolnego znajomego ktorym jest ten użytkownik.
# U1, [U2, U3, U4] => U2-U3, U2-U4, U3-U4 - Kazda para ma 1 wspolnego znajomego U1.
# Prowadząc takie rozbicie dla wszystkich użytkowników otrzymamy listę par posiadających wspolnych znajomych.(Będą zduplikowane wartości).
# Po pogrupowaniu i zliczeniu otrzymamy <para><ilosc_wspolnych_znajomych>

# Tylko użytkownicy ze znajomymi są brani pod uwagę - UsersWithFriendsMap
# Do utowrzenia kombinacji użyto itertools i combinations
# Finalnie otrzymamy ((U1,U2),1) => U1,U2 - para użytkowników mających 1 wspolnego znajomego.

matchedFriends = UsersWithFriendsMap.map(M.combineFriends).flatMap(lambda x: x)

print("matchedFriends")
sam1 = matchedFriends.take(5)
print(sam1)


# Tutaj następuje grupowanie opisane wyżej. Za pomocą reduceByKey, dla tych samych kluczy obliczana jest suma wystąpień.
matchedFriendsWithCount = matchedFriends.reduceByKey(lambda accumulatedValue, currentValue: accumulatedValue + currentValue)

print("matchedFriendsWithCount")
sam2 = matchedFriendsWithCount.take(5)
print(sam2)


# Wsród tych par znajdują się takie które są już znajomymi. Należy je usunąć. 
# Do tego użyjemy danych z UsersWithFriendsMap

UserFriendDictionary = UsersWithFriendsMap.collectAsMap()

print("UserFriendDictionary")
print("DONE")

# Filtrujemy znajomych którzy są już znajomymi. Nie rekomendujemy znajomości dla już zeprzyjaźnionych par.
# Działanie: dla danego elementu z matchedFriendsWithCount: ((1,11), 2)
# item[0][0] = 1("uzytkownik"), item[0][1] = 11 ("znajomy"), item[1] = 2
# Znajomi, ktorzy nie sa w znajomych tego użytkownika
matchedFriendsWithCountWOSingle = matchedFriendsWithCount.filter(lambda item: item[0][1] not in UserFriendDictionary[item[0][0]])

print("matchedFriendsWithCountWOSingle")
sam4 = matchedFriendsWithCountWOSingle.take(5)
print(sam4)


# Kolejnym etapem będzie stworznie każdemu użytkownikowi listy pogrupowanej według ilosci wspolnych znajmoych + ci znajomi
# Chcemy otrzymać wynik w postaci U1 - 5 : [...],  3 : [...] itd.
# Aby to otrzymać poprzednio otrzymane pary z ilością wspolnych znajomych rozdzielamy by otrzymać "Użytkownika + ilosc wspolnych znajmoych + z jakim użytkownikiem"
# Rozdzielamy utworzone pary, tak, aby otrzymać użytkownika wraz z informają ile on ma wspolnych znajomych z danym użytkownikiem.
# ((U1,U2), 3) => (U1, {3,U2}) (U2, {3, U1})
UserWithCntAndFriend = matchedFriendsWithCountWOSingle.map(lambda pC: [(pC[0][0], {pC[1]: [pC[0][1]]}), (pC[0][1], {pC[1]: [pC[0][0]]})]).flatMap(lambda x: x)
UserWithCntAndFriend

print("UserWithCntAndFriend")

# Następnie musimy połączyć tych znajomych ktorzy mają tę samą liczę wspolnych znajomych
# (1,{2,[8]}), (1,{2,[4]}), (1,{2,[7]})  => (1, {2,[8,4,7]})
# Używamy reduceByKey i zdefiniowanej funkcji. 
# Dla elementów z tymi samymi kluczami- dodajemy wartość spod klucza do tablicy, jesli nie ma takiego klucza, to tworzymy nową tablice z tą wartością.

UserWithCntAndFriendList = UserWithCntAndFriend.reduceByKey(M.transform)

print("UserWithCntAndFriendList")
sam5 = UserWithCntAndFriendList.take(5)
print(sam5)


# Zgodnie z wymaganiami dane musza być posortowane malejąca po ilości znajomych i rosnąco w ramach grupy znajomych.
# (U1, {3, [U4, U4], 6 [U7, U8, U9]}) => (U1, [ (6, [U7, U8, U9]), (3, [U3, U4] ])  ) 
RecommendationWithCnt = UserWithCntAndFriendList.map(M.Sorting)

print("RecommendationWithCnt")
sam6 = RecommendationWithCnt.take(1)

# Zgodnie z wymaganiemi, ilosc wspolnych znajomych jest niestotna na tym etapie.
# Zmieniamy format (U1, [ (3, [U3, U4]), (2, [U6, U9, U11]) ] ) usuwając ilości i łącząc listy.
# Oczekiwanie: (U1, [U3, U4, U6, U9])

RecommendationListWOCnt = RecommendationWithCnt.map(M.reduceAndMergeList)

# Ostatecznie tworzymy dictionary w postaci {U : [U1, U2,...]}
# Aby łatwo zapisac dane do pliku lub je wyświetlic.
FinalRecommendation = RecommendationListWOCnt.collectAsMap()

print("FinalRecommendation")
print("DONE")

def DataToFile(RecomMap, UserKeyList, N, fileName):
    stream = open(fileName, 'w+')
    for key in UserKeyList:
        if key in RecomMap:
            length = min(len(RecomMap[key]), N)
            s = ','.join(str(e) for e in RecomMap[key][:length])
            print(str(key) +"\t"+ str(s),file=stream)
        else:
            print(str(key), file=stream)
    stream.close()


print("Saving...")

if Choice == 1:
    DataToFile(FinalRecommendation, AllUsersSortedList, AmountOfRec, 'AllUsersOutput.txt')
elif Choice == 2:
    DataToFile(FinalRecommendation, CustomList, AmountOfRec, 'CustomUserListOutput.txt')
elif Choice == 3:
    TemplateUserList = [924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]
    DataToFile(FinalRecommendation, TemplateUserList, AmountOfRec, 'TemplateUserListOutput.txt')
elif Choice == 4:
    UserNo11 = [11]
    DataToFile(FinalRecommendation, UserNo11, AmountOfRec, 'UserNo11Output.txt')
else:
    print("error")

print("Saved")
    
sc.stop()

print("The End")