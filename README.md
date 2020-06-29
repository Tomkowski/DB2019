# DB2019

checkIDtaken(id) – sprawdza czy w bazie istnieje już użytkownik bądź inny obiekt o zadanym ‘id’.
checkCredentials(id, pswrd) – sprawdza, czy w bazie istnieje użytkownik o zadanym id i haśle.
checkFrozen(timestamp, memberid) – sprawdza, czy użytkownik wykonał na koncie jakąś operację w przeciągu ostatniego roku
updateTimestamp(timestamp, memberID) – aktualizuje czas ostatniej operacji na koncie o zadanym ‘id’
addMember(js, isLeader) – dodaje do bazy członka o właściwościach
zadanych w obiekcie JSON ‘js” (isLeader informuje, czy członek ma być liderem)
getProjectAuthority(projectID) – zwraca id lidera który utworzył projekt

W poniższych metodach definicja jest taka sama co w specyfikacji zadania.
createProject(projectID, authority)
addSuport(js)
addProtest(js)
vote(js, type)
votes(js)
trolls(js)
projects(js)
actions(js)
requestOpen(js) – próbuje nawiązać połączenenie z serwerem psql.
Tworzy wtedy zmienne globalne, które wykorzystują następnie pozostałe metody.
executeQueries() - w nieskończonej pętli program oczekuje na wprowadzenie przez użytkownika obiektu JSON. W zależności od słownikowego klucza metoda wykonuje odpowiednie zapytania opisane na górze
