Python scripts to aid in the downloading and processing of ICESat-2 data files

download_data.py is provided by the nsidc, with some changes by me.

process_data.py is was created by me. It is meant to assist in the processing of ICESat-2 .h5 data files on a single computer. By using a modified binary search to find bounds of the relevant data in the files, we can save a lot of processing time. 

The only drawback is that sometimes unwanted data can get past us. For example, let's say we want data between 20 degrees and 30 degrees latitude. When we find the first instance of 20 degrees latitude in the data, there might be about 20 rows of data that come after that will be at 19.999992 degrees latitude. I believe the tradeoff is worth it for the amount of time saved as we can always go through the data later and remove these rows. This is only really a problem when we stop looking at the first instance of 30 degrees (for this example). There could be 10s of rows after that contain data we may want. In order to make sure this doesn't happen, just add .001 degrees of wiggle room to the bounding box. So 20 - 30 degrees becomes 19.999 - 30.001 degrees. Yes you do get more data, but that can always be fixed later.

More info on [ICESat-2](https://nsidc.org/data/ATL03/versions/2)

