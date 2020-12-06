# World Energy Balance Dashboard

This work is done by Ben Xiong, Frances Niu, Sayan Samanta, Christina Ye for the partial fulfillment of the course requirements for DATA 1050 - Data Engineering Fall 2020 @ Brown University. We thank Prof. Dan Potter and the TAs for the learning resources.

## Summary

In this project, we designed a dashboard where it compares the different modes of transactions (imports, exports, consumption etc.) of energy (in units of Tera Joules) that is obtained from several commodities such as Oil Products, Natural Gas, Electricity etc. The comparison between all countries in regards to different commodities with respect to different transactions can be done at a glance via a coloured World Map. Further scrutiny provides the temporal change of the selected commodity-transaction pair between 1990 and now. The data is also supplemented with the greenhouse gas emission data of the country (if available). Further, if the requirement is to study the performance of a selected country at a given year, we provide a set of charts which exposes the preference of transactions for a particular commodity. In addition, we exhibit the preference of the transaction among different commodities. Lastly, based on the average consumption data, we also predict the next year's figures of any commodity in any transaction, based on available previous data from all countries.

## Steps to run locally

#### Clone the repository using

```bash
$ git clone https://github.com/reach2sayan/data-1050-infinity.git
$ cd data-1050-infinity
```
#### Install the python dependencies

```bash
$ pip install -r requirements.txt
```

#### Run the file

```bash
$ python wsgi.py
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)
