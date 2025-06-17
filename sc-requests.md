```
curl 'https://scr.sci.gov.in/scrsearch/?p=pdf_search/checkCaptcha' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
  -b 'SCR_SESSID=ebbhiqkfsg3g6vqonl34kuieok; __session:0.9456032029069489:0_path=2008_2_95_100; __session:0.9456032029069489:citation_year=2008; JSESSION=11215955' \
  -H 'DNT: 1' \
  -H 'Origin: https://scr.sci.gov.in' \
  -H 'Pragma: no-cache' \
  -H 'Referer: https://scr.sci.gov.in/' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36' \
  -H 'X-Requested-With: XMLHttpRequest' \
  -H 'sec-ch-ua: "Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  --data-raw 'captcha=yech93&search_text=&search_opt=PHRASE&escr_flag=&proximity=&sel_lang=&neu_cit_year=&neu_no=&ncn=&citation_vol=&citation_year=&citation_supl=&citation_page=&ajax_req=true&app_token='
```

```
curl 'https://scr.sci.gov.in/scrsearch/?p=pdf_search/home&text=&captcha=yech93&search_opt=PHRASE&fcourt_type=undefined&escr_flag=&proximity=&sel_lang=&neu_cit_year=&neu_no=&ncn=&citation_vol=&citation_yr=&citation_supl=&citation_page=&app_token=curl 'https://scr.sci.gov.in/scrsearch/?p=pdf_search/home/' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
  -b 'SCR_SESSID=rejcqlaakrcqm5b0hr0hoc6nrt; __session:0.9456032029069489:0_path=2008_2_95_100; __session:0.9456032029069489:citation_year=2008; __session:0.7590650764503304:38_path=2025_4_120_128; __session:0.7590650764503304:citation_year=2025; JSESSION=76245148' \
  -H 'DNT: 1' \
  -H 'Origin: https://scr.sci.gov.in' \
  -H 'Pragma: no-cache' \
  -H 'Referer: https://scr.sci.gov.in/' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36' \
  -H 'X-Requested-With: XMLHttpRequest' \
  -H 'sec-ch-ua: "Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  --data-raw '&sEcho=1&iColumns=2&sColumns=,&iDisplayStart=0&iDisplayLength=10&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&search_txt1=&search_txt2=&search_txt3=&search_txt4=&search_txt5=&pet_res=&state_code=&state_code_li=&dist_code=null&case_no=&case_year=&from_date=&to_date=&judge_name=&reg_year=&fulltext_case_type=&act=&judge_txt=&act_txt=&section_txt=&judge_val=&act_val=&year_val=&judge_arr=&flag=&disp_nature=&search_opt=PHRASE&date_val=ALL&fcourt_type=3&citation_yr=&citation_vol=&citation_supl=&citation_page=&case_no1=&case_year1=&pet_res1=&fulltext_case_type1=&citation_keyword=&sel_lang=&proximity=&neu_cit_year=&neu_no=&ncn=&bool_opt=&sort_flg=&ajax_req=true&app_token='
```

Clicking on a judgment

```
curl 'https://scr.sci.gov.in/scrsearch/?p=pdf_search/openpdfcaptcha' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
  -b 'SCR_SESSID=jtmqe2vpe0ippnj9btcoo2j3po; __session:0.9456032029069489:0_path=2008_2_95_100; __session:0.9456032029069489:citation_year=2008; JSESSION=11215955; __session:0.7590650764503304:=https:' \
  -H 'DNT: 1' \
  -H 'Origin: https://scr.sci.gov.in' \
  -H 'Pragma: no-cache' \
  -H 'Referer: https://scr.sci.gov.in/' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36' \
  -H 'X-Requested-With: XMLHttpRequest' \
  -H 'sec-ch-ua: "Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  --data-raw 'val=0&lang_flg=undefined&path=2025_5_275_330&citation_year=2025&fcourt_type=3&nc_display=2025INSC555&ajax_req=true&app_token='
```

Downloading PDF
```
curl 'https://scr.sci.gov.in/scrsearch/tmp/b8e9427d1d2f6ddcd42f9ae60cc079150c095becaccadd8169d42ed805fcd0701749434230.pdf' \
  -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7' \
  -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -b 'SCR_SESSID=jtmqe2vpe0ippnj9btcoo2j3po; __session:0.9456032029069489:0_path=2008_2_95_100; __session:0.9456032029069489:citation_year=2008; JSESSION=11215955; __session:0.7590650764503304:=https:' \
  -H 'DNT: 1' \
  -H 'Pragma: no-cache' \
  -H 'Referer: https://scr.sci.gov.in/' \
  -H 'Sec-Fetch-Dest: object' \
  -H 'Sec-Fetch-Mode: navigate' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'Sec-Fetch-User: ?1' \
  -H 'Upgrade-Insecure-Requests: 1' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36' \
  -H 'sec-ch-ua: "Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"'
```

Another language judgment "PUN"

```
curl 'https://scr.sci.gov.in/scrsearch/?p=pdf_search/openpdfcaptcha' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
  -b 'SCR_SESSID=ua2dv736k5pvmuanc9ojh3o1rq; __session:0.9456032029069489:0_path=2008_2_95_100; __session:0.9456032029069489:citation_year=2008; JSESSION=11215955; __session:0.7590650764503304:38_path=2025_4_120_128; __session:0.7590650764503304:citation_year=2025' \
  -H 'DNT: 1' \
  -H 'Origin: https://scr.sci.gov.in' \
  -H 'Pragma: no-cache' \
  -H 'Referer: https://scr.sci.gov.in/' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36' \
  -H 'X-Requested-With: XMLHttpRequest' \
  -H 'sec-ch-ua: "Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  --data-raw 'val=38&lang_flg=PUN&path=2025_4_120_128&citation_year=2025&fcourt_type=3&nc_display=2025INSC373&ajax_req=true&app_token='
```

Normal judgment html
```
<tr class="odd"><td class="sorting_1 dtr-control">1</td><td><button type="button" role="link" class="btn btn-link p-0 text-start" id="link_0" aria-label=" State of Uttar Pradesh through Principal Secretary, Department of Panchayati Raj, Lucknow versus Ram Prakash Singh pdf" href="#" onclick="javascript:open_pdf('0','2025','2025_5_275_330','2025INSC555');"><font size="4"> <strong>STATE OF UTTAR PRADESH THROUGH PRINCIPAL SECRETARY, DEPARTMENT OF PANCHAYATI RAJ, LUCKNOW<span class="fst-italic"> versus </span>RAM PRAKASH SINGH </strong>- <span class="escrText">[2025] 5 S.C.R. 275</span><span class="ncDisplay">2025 INSC 555</span><input type="hidden" id="cnr" value="ESCR010001462025"></font></button><br><strong>Coram : DIPANKAR DATTA<sup style="color: #268e97;font-size: 22px;top: 0;" class="tooltip-sup" data-tooltip="Author">*</sup>, PRASHANT KUMAR MISHRA</strong><br> Issue for Consideration Whether, in pursuance of a purported enquiry where there was none to present the case of the department, no witness was examined in support of the charges and no document was formally proved, any order of be made; whether the disciplinary authority was justified in placing reliance on a report of enquiry prepared by the Enquiry Officer who had looked into documents which were not provided to the respondent and had arrived at findings of guilt only on the basis of the charge- sheet, the reply<br><strong class="caseDetailsTD"><span style="color:#212F3D"> Decision Date :</span><font color="green"> 23-04-2025</font><span style="color:#212F3D"> | Case No :</span><font color="green"> CIVIL APPEAL No. 14724/2024</font><span style="color:#212F3D"> | Disposal Nature :</span><font color="green"> Dismissed</font>   <span style="color:#212F3D"> |  Bench :</span><font color="green"> 2 Judges</font></strong><div class="row my-1 mt-2 border bg-light"><div class="col-auto"><a href="#?app_token=" class="btn btn-outline-secondary btn-sm my-1" onclick="open_splitview('0','2025','2025_5_275_330','2025INSC555','S')"><i <i="" class="fas fa-columns me-2"></i>Split view</a></div><div class="col-auto"><a href="#?app_token=" class="btn btn-outline-secondary btn-sm my-1" onclick="open_splitview('0','2025','2025_5_275_330','2025INSC555','H');"><i class="fas fa-file-alt me-2"></i>HTML view</a></div><div class="col-auto"><a href="#?app_token=" class="btn btn-outline-secondary btn-sm my-1" onclick="open_flip('0','2025','2025_5_275_330','2025INSC555');"><i class="fas fa-book-open me-2"></i>Flip view</a></div><div class="col-auto"><a href="#?app_token=" class="btn btn-outline-secondary btn-sm my-1" onclick="open_pdf('0','2025','2025_5_275_330','2025INSC555','N');"><i class="fas fa-file-pdf me-2"></i>PDF</a></div></div></td></tr>
```

Multi language judgment HTML

```
<tr class="even"><td class="sorting_1 dtr-control">34</td><td><select class="form-select form-select-sm me-2" id="language33" name="language" onchange="javascript:get_pdf_lang('33','2025_4_144_155','2025');" style="display: inline; width: 90px !important;background-color: #89c8bd;">
							<option value="">English</option>	<option value="PUN">ਪੰਜਾਬੀ - Punjabi</option></select> <br>
						<div class="modal fade" id="DisclaimerModal" data-bs-backdrop="static" data-bs-keyboard="false" tabindex="-1" aria-labelledby="DisclaimerModalLabel" aria-hidden="true">
						  <div class="modal-dialog">
							<div class="modal-content">
							  <div class="modal-header">
								<h5 class="modal-title" id="DisclaimerModalLabel"> Disclaimer </h5>
								<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
							  </div>
							  <div class="modal-body"> <p>
								Due care and caution has been taken by the Editorial Section, Supreme Court of India to provide complete and accurate information in the English version. 
						Judgments in  regional languages are also made available alongside the English version. The translations are being done with the help of various human agencies and software tools. 
						Reasonable efforts have been made to provide an accurate translation.
						 
						 Hindi Judgment as published in Ucchtam Nyayalya Nirnay Patrika are reproduced under approval of Vidhi Sahitya Prakashan, Department of legislative affairs, Govt. Of India. 
						 
						Supreme Court Registry will not responsible for incorrect or inaccurate translation and  for any error, omission or discrepancy in the content of translated text. 
						The translation of Judgements is provided for general information  and shall have no legal effect for compliance or enforcement.
						If any questions arise related to the accuracy of the information/statement contained in the translated judgment,  users are advised to verify from the original judgments and also to refer to correct position of law while referring to old judgments. </p><p>
						Visitors to the site are requested to cross check the correctness of the information on this site with the authorities concerned or consult the relevant record. The information made available here is not meant for legal evidence. Neither the Courts concerned nor the National Informatics Centre (NIC) nor the e-Committee is responsible for any data inaccuracy or delay in the updation of the data on this website. We do not accept any responsibility or liability for any damage or loss arising from the direct/indirect use of the information provided on the site. However, we shall be obliged if errors/omissions are brought to our notice for carrying out the corrections.
						</p></div></div></div></div><button type="button" role="link" class="btn btn-link p-0 text-start" id="link_33" aria-label=" State of Madhya Pradesh versus Shyamlal &amp; Ors.  pdf" href="#" onclick="javascript:open_pdf('33','2025','2025_4_144_155','2025INSC377');"><font size="4"> <strong>STATE OF MADHYA PRADESH<span class="fst-italic"> versus </span>SHYAMLAL &amp; ORS.  </strong>- <span class="escrText">[2025] 4 S.C.R. 144</span><span class="ncDisplay">2025 INSC 377</span><input type="hidden" id="cnr" value="ESCR010001082025"></font></button><br><strong>Coram : ABHAY S. OKA<sup style="color: #268e97;font-size: 22px;top: 0;" class="tooltip-sup" data-tooltip="Author">*</sup>, AHSANUDDIN AMANULLAH, AUGUSTINE GEORGE MASIH</strong><br> Issue for Consideration The High Court converted the conviction of respondents u/s.302 of IPC into the second part of s.304 of the IPC. Whether the judgment of the High Court requires interference. Headnotes† Penal Code, 1860 – s.302 and second part of s.304 – The case of the accused, with a common intention and object, came together and assaulted PW-1, PW-2, PW-3, PW-11, PW-12 and the deceased, on 01.11.1989 – The Trial Court convicted the respondents for the offences punishable u/s.147 and ss.452, 302, 325, and 323 r/w. s.149 of the IPC – By the<br><strong class="caseDetailsTD"><span style="color:#212F3D"> Decision Date :</span><font color="green"> 20-03-2025</font><span style="color:#212F3D"> | Case No :</span><font color="green"> CRIMINAL APPEAL No. 1254/2024</font><span style="color:#212F3D"> | Disposal Nature :</span><font color="green"> Dismissed</font>   <span style="color:#212F3D"> |  Bench :</span><font color="green"> 3 Judges</font></strong><div class="row my-1 mt-2 border bg-light"><div class="col-auto"><a href="#?app_token=" class="btn btn-outline-secondary btn-sm my-1" onclick="open_splitview('33','2025','2025_4_144_155','2025INSC377','S')"><i <i="" class="fas fa-columns me-2"></i>Split view</a></div><div class="col-auto"><a href="#?app_token=" class="btn btn-outline-secondary btn-sm my-1" onclick="open_splitview('33','2025','2025_4_144_155','2025INSC377','H');"><i class="fas fa-file-alt me-2"></i>HTML view</a></div><div class="col-auto"><a href="#?app_token=" class="btn btn-outline-secondary btn-sm my-1" onclick="open_flip('33','2025','2025_4_144_155','2025INSC377');"><i class="fas fa-book-open me-2"></i>Flip view</a></div><div class="col-auto"><a href="#?app_token=" class="btn btn-outline-secondary btn-sm my-1" onclick="open_pdf('33','2025','2025_4_144_155','2025INSC377','N');"><i class="fas fa-file-pdf me-2"></i>PDF</a></div></div></td></tr>
```                        