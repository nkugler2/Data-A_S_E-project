# SEC Requirements
*This is information that I gather over time about specific requirements that the SEC has when using their data*

## Header 

SEC wants to know (apparently) who is pulling their data. So, I need to have a `header` that shows my name and my email address. An example would be:

```python
headers = {
    'User-Agent': 'John Smith jsmith@university.edu'
}
```

## Rate Limit

The SEC has another rule: **A Maximum of 10 requests per second.** This is not an issue for this file, as I will only be downloading one file as a test, but will be important to keep in mind when I make a script to download multiple files as I build this project up


