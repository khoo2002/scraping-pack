import DownloadSaveAsLocalPDF
import requests
str = "https://sebenarnya.my/wp-sitemap-posts-post-{}.xml"
numberPage = 1
while True:
    response = requests.get(str.format(numberPage))
    if response.status_code == 200:
        xml = response.text
        xml = xml.split("<loc>")
        print("Page ", numberPage)
        print("Total links: ", len(xml))
        for i in range(1, 2):
            link = xml[i].split("</loc>")[0]
            print(link)
            DownloadSaveAsLocalPDF.DownloadSaveAsLocalPDF(link, r"C:\Users\khooz\Downloads\Internship\scraping-pack", 3)
        numberPage += 1
    else:
        numberPage -= 1
        break
print("Total page: ", numberPage)


