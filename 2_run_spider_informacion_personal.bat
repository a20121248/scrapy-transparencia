call C:\ProgramData\Anaconda3\Scripts\activate.bat C:\ProgramData\Anaconda3
call conda activate scrapy_01
scrapy crawl informacion_personal -a codmes=201810
scrapy crawl informacion_personal -a codmes=201811
pause
