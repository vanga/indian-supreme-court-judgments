Download the cleaned metadata from [here](./data/metadata/clean/judgments.csv)
PDF judgmetns and the metadata can be downloaded from [Kaggle](https://www.kaggle.com/datasets/vangap/indian-supreme-court-judgments/data) which gets updated weekly.


* Gets metadata about judgments from API. The details of API are figured out by inspecting the android app traffic.
* Sample request
    ```
    """
    curl --location 'https://scourtapp.nic.in/?pageid=100001' \
    --header 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'rpt_type=A' \
    --data-urlencode 'from_date=01-01-2021' \
    --data-urlencode 'to_date=30-11-2022' \
    --data-urlencode 'token=<token-here>' \
    --data-urlencode 'judgename=99999'
    """
    ```
* There are some judgments that are returned under 1902 year, which actually belong to other years like 2002. This must be an issue in the data.
* Intervals are chosen to optimize the number of files and size of each file.
* Even the old years are queried every time, to see if any changes take place in the old years' data. It has been observed that new entries are added as part of old years as well. Git commit history shall show us how often this is happenning over time.
* Some of the links like "judis/44700.pdf" need to be prefixed with "jonew/" to get a working url
* Metadata also contains examples where the same judgment is part of multiple years. For ex, diary-no 17050-2006 appears with two judgment dates while the judgment pdf link is the same.

## Dataset Size by Year

| Year | Size (GB) |
|------|-----------|
| 1950 | 0.095 |
| 1951 | 0.137 |
| 1952 | 0.138 |
| 1953 | 0.128 |
| 1954 | 0.189 |
| 1955 | 0.349 |
| 1956 | 0.114 |
| 1957 | 0.115 |
| 1958 | 0.183 |
| 1959 | 0.317 |
| 1960 | 0.404 |
| 1961 | 0.434 |
| 1962 | 0.796 |
| 1963 | 0.436 |
| 1964 | 0.737 |
| 1965 | 0.559 |
| 1966 | 0.597 |
| 1967 | 0.377 |
| 1968 | 0.286 |
| 1969 | 0.312 |
| 1970 | 0.287 |
| 1971 | 0.508 |
| 1972 | 0.345 |
| 1973 | 0.495 |
| 1974 | 0.537 |
| 1975 | 0.491 |
| 1976 | 0.450 |
| 1977 | 0.375 |
| 1978 | 0.434 |
| 1979 | 0.395 |
| 1980 | 0.377 |
| 1981 | 0.342 |
| 1982 | 0.357 |
| 1983 | 0.296 |
| 1984 | 0.337 |
| 1985 | 0.571 |
| 1986 | 0.322 |
| 1987 | 0.302 |
| 1988 | 0.531 |
| 1989 | 0.413 |
| 1990 | 0.472 |
| 1991 | 0.525 |
| 1992 | 0.535 |
| 1993 | 0.584 |
| 1994 | 0.675 |
| 1995 | 1.300 |
| 1996 | 2.507 |
| 1997 | 1.372 |
| 1998 | 0.757 |
| 1999 | 0.782 |
| 2000 | 0.646 |
| 2001 | 0.725 |
| 2002 | 0.844 |
| 2003 | 0.894 |
| 2004 | 0.778 |
| 2005 | 0.573 |
| 2006 | 0.797 |
| 2007 | 1.201 |
| 2008 | 1.443 |
| 2009 | 0.639 |
| 2010 | 1.101 |
| 2011 | 1.041 |
| 2012 | 0.841 |
| 2013 | 1.047 |
| 2014 | 0.939 |
| 2015 | 0.857 |
| 2016 | 0.715 |
| 2017 | 0.948 |
| 2018 | 1.006 |
| 2019 | 1.086 |
| 2020 | 1.686 |
| 2021 | 1.894 |
| 2022 | 2.271 |
| 2023 | 2.056 |
| 2024 | 1.734 |
| 2025 | 0.104 |

**Total Size Across All Years: 52.244 GB**


Shield: [![CC BY 4.0][cc-by-shield]][cc-by]

This work is licensed under a
[Creative Commons Attribution 4.0 International License][cc-by].

[![CC BY 4.0][cc-by-image]][cc-by]

[cc-by]: http://creativecommons.org/licenses/by/4.0/
[cc-by-image]: https://i.creativecommons.org/l/by/4.0/88x31.png
[cc-by-shield]: https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg