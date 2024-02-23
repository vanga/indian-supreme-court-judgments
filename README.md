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



Shield: [![CC BY 4.0][cc-by-shield]][cc-by]

This work is licensed under a
[Creative Commons Attribution 4.0 International License][cc-by].

[![CC BY 4.0][cc-by-image]][cc-by]

[cc-by]: http://creativecommons.org/licenses/by/4.0/
[cc-by-image]: https://i.creativecommons.org/l/by/4.0/88x31.png
[cc-by-shield]: https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg
