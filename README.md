Please read before execute.

1) mk_pass.sh
 : Make encrypted password

 Usage
$ sh mk_pass.sh user_password

output :
  KEY
  ENCRYPT KEY
  DECRYPT KEY




2) compile.sh
 : Compile modified source

 Usgae
$ sh compile.sh




3) load.sh
 : Load data from file to target database
 : You can load data where you want by choosing database [ Modify load.sh ]

 Usage
$ sh load.sh

 3 - 1) conf/loader.json
        conf/loader_sundb.json

		You should enter the information of database.



4) export.sh
 : export data from source database to file
 : You can export data by choosing database [ Modify export.sh ]

 Usage
$ sh export.sh

 4 - 1) conf/export_ora.json
        conf/export.json

		You should enter the information of database.
