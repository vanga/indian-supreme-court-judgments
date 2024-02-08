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