# utl-join-yearly-rainfall-in-one-table-with-yearly-temperature-in-another-using-normalization
Join yearly rainfall in one table with yearly temperature in another table using normailzation
    %let pgm=utl-join-yearly-rainfall-in-one-table-with-yearly-temperature-in-another-using-normalization;

    Join yearly rainfall in one table with yearly temperature in another table using normailzation

     1 wps sql
     2 wps r sql
     3 wps python sql


    github
    https://tinyurl.com/4u3k26dz
    https://github.com/rogerjdeangelis/utl-join-yearly-rainfall-in-one-table-with-yearly-temperature-in-another-using-normalization

    stackoverflow R
    https://tinyurl.com/bdew8n64
    https://stackoverflow.com/questions/77052848/combining-two-dataframes-based-on-two-variables-when-one-variable-is-a-single-co

    /*                   _
    (_)_ __  _ __  _   _| |_ ___
    | | `_ \| `_ \| | | | __/ __|
    | | | | | |_) | |_| | |_\__ \
    |_|_| |_| .__/ \__,_|\__|___/
            |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";

    data sd1.temp;
    informat country $10.;
    input Country Year Temperature;
    cards4;
    Germany 2010 20
    Germany 2009 19
    Germany 2008 18
    Australia 2010 24
    Australia 2009 23
    Australia 2018 22
    Spain 2010 23
    Spain 2009 24
    Spain 2008 21
    ;;;;
    run;quit;

    /*---- I normalized the second table to look like the first              ----*/
    data sd1.rain;
      informat country $10.;
      input Country rain1-rain3;
      array rains rain1-rain3;
      year = 2010;rain=rain1;output;
      year = 2009;rain=rain2;output;
      year = 2008;rain=rain3;output;
      keep country year rain;
    cards4;
    Germany 800 900 1000
    Australia 700 600 750
    Spain 800 650 550
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                  |                                                     */
    /*                               INPUTS                             |                   OUTPUTS                           */
    /*                                                                  |                                                     */
    /*  SD1.TEMP                              SD1.RAIN                  |   SD1.WANY                                          */
    /*  ================================      ========================= |   -------------------------------------------       */
    /*  COUNTRY      YEAR    TEMPERATURE      COUNTRY      YEAR    RAIN |   COUNTRY         YEAR  TEMPERATURE      RAIN       */
    /*                                                                  |                                                     */
    /*  Germany      2010         20          Germany      2010     800 |   Germany         2010           20       800       */
    /*  Germany      2009         19          Germany      2009     900 |   Germany         2009           19       900       */
    /*  Germany      2008         18          Germany      2008    1000 |   Germany         2008           18      1000       */
    /*  Australia    2010         24          Australia    2010     700 |   Australia       2010           24       700       */
    /*  Australia    2009         23          Australia    2009     600 |   Australia       2009           23       600       */
    /*  Australia    2018         22          Australia    2008     750 |   Spain           2010           23       800       */
    /*  Spain        2010         23          Spain        2010     800 |   Spain           2009           24       650       */
    /*  Spain        2009         24          Spain        2009     650 |   Spain           2008           21       550       */
    /*  Spain        2008         21          Spain        2008     550 |                                                     */
    /*                                                                  |                                                     */
    /**************************************************************************************************************************/

    /*                                  _
    / | __      ___ __  ___   ___  __ _| |
    | | \ \ /\ / / `_ \/ __| / __|/ _` | |
    | |  \ V  V /| |_) \__ \ \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
    */

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc sql;
      select
         l.*
        ,r.rain
      from
         sd1.temp as l inner join sd1.rain as r
      on
              l.country = r.country
         and  l.year    = r.year
    ;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /*   COUNTRY         YEAR  TEMPERATURE      RAIN                                                                          */
    /*   -------------------------------------------                                                                          */
    /*   Germany         2010           20       800                                                                          */
    /*   Germany         2009           19       900                                                                          */
    /*   Germany         2008           18      1000                                                                          */
    /*   Australia       2010           24       700                                                                          */
    /*   Australia       2009           23       600                                                                          */
    /*   Spain           2010           23       800                                                                          */
    /*   Spain           2009           24       650                                                                          */
    /*   Spain           2008           21       550                                                                          */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                                          _
    |___ \  __      ___ __  ___   _ __   ___  __ _| |
      __) | \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
     / __/   \ V  V /| |_) \__ \ | |    \__ \ (_| | |
    |_____|   \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                     |_|                        |_|
    */
    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    options validvarname=any;
    libname sd1 "d:/sd1";

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.temp r=temp;
    export data=sd1.rain r=rain;
    submit;
    library(sqldf);
    want <-sqldf("
      select
         l.*
        ,r.rain
      from
         temp as l inner join rain as r
      on
              l.country =  r.country
         and  l.year    = r.year
      ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;\
    ');

    proc print data=sd1.want width=min;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* R                                                                                                                      */
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /*     COUNTRY YEAR TEMPERATURE RAIN                                                                                      */
    /* 1   Germany 2010          20  800                                                                                      */
    /* 2   Germany 2009          19  900                                                                                      */
    /* 3   Germany 2008          18 1000                                                                                      */
    /* 4 Australia 2010          24  700                                                                                      */
    /* 5 Australia 2009          23  600                                                                                      */
    /* 6     Spain 2010          23  800                                                                                      */
    /* 7     Spain 2009          24  650                                                                                      */
    /* 8     Spain 2008          21  550                                                                                      */
    /*                                                                                                                        */
    /* WPS                                                                                                                    */
    /*                                                                                                                        */
    /*    COUNTRY      YEAR    TEMPERATURE    RAIN                                                                            */
    /*                                                                                                                        */
    /*    Germany      2010         20         800                                                                            */
    /*    Germany      2009         19         900                                                                            */
    /*    Germany      2008         18        1000                                                                            */
    /*    Australia    2010         24         700                                                                            */
    /*    Australia    2009         23         600                                                                            */
    /*    Spain        2010         23         800                                                                            */
    /*    Spain        2009         24         650                                                                            */
    /*    Spain        2008         21         550                                                                            */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                                    _   _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | |
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_|
                     |_|         |_|    |___/
    */
    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    libname sd1 'd:/sd1';
    proc python;
    export data=sd1.temp python=temp;
    export data=sd1.rain python=rain;
    submit;
    from os import path;
    import pandas as pd;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    want = pdsql('''
      select
         l.*
        ,r.rain
      from
         temp as l inner join rain as r
      on
              l.country =  r.country
         and  l.year    = r.year
      ''');
    print(want);
    endsubmit;
    import data=sd1.want python=want;
    run;quit;
    proc print data=sd1.want width=min;
    run;quit;
    ");

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The PYTHON Procedure                                                                                                   */
    /*                                                                                                                        */
    /*       COUNTRY    YEAR  TEMPERATURE    RAIN                                                                             */
    /* 0  Germany     2010.0         20.0   800.0                                                                             */
    /* 1  Germany     2009.0         19.0   900.0                                                                             */
    /* 2  Germany     2008.0         18.0  1000.0                                                                             */
    /* 3  Australia   2010.0         24.0   700.0                                                                             */
    /* 4  Australia   2009.0         23.0   600.0                                                                             */
    /* 5  Spain       2010.0         23.0   800.0                                                                             */
    /* 6  Spain       2009.0         24.0   650.0                                                                             */
    /* 7  Spain       2008.0         21.0   550.0                                                                             */
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* Obs     COUNTRY     YEAR    TEMPERATURE    RAIN                                                                        */
    /*                                                                                                                        */
    /*  1     Germany      2010         20         800                                                                        */
    /*  2     Germany      2009         19         900                                                                        */
    /*  3     Germany      2008         18        1000                                                                        */
    /*  4     Australia    2010         24         700                                                                        */
    /*  5     Australia    2009         23         600                                                                        */
    /*  6     Spain        2010         23         800                                                                        */
    /*  7     Spain        2009         24         650                                                                        */
    /*  8     Spain        2008         21         550                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
