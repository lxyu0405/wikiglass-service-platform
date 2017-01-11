# -*- coding: utf-8 -*-
import xlrd

'''
Following codes are used to extract data from Excel file, and generate sql statements.
Usage: python ExData_kyc.py.py > file_name.sql
'''


def insert_into_wiki_table(wiki_id, wiki_url, year, school, grade, wiki_class, group_no, class_name, admin_key):
    query = "INSERT INTO Wiki (wiki_id, wiki_url, year, school, grade, class, group_no, class_name, admin_key)" +\
            " VALUES(\'" + wiki_id + "\'," +\
            "\'" + wiki_url + "\'," +\
            " " + year + "," +\
            " \'" + school + "\'," +\
            " " + grade + "," +\
            " \'" + wiki_class + "\'," +\
            " \'" + group_no + "\'," +\
            " \'" + class_name + "\'," +\
            " \'" + admin_key + "\');"
    print(query)


def update_wiki_table(wiki_url, group, wiki_id, class_id):
    query = "UPDATE Wiki SET group_no = " + group + ", class_name = \'" + class_id + "\', wiki_id = \'" + wiki_id + "\' WHERE wiki_url = \'" + wiki_url +"\';"
    print(query)


def extract_data_from_excel_by_sheet_name(workbook, sheet_name):
    wiki_class = sheet_name[-1].lower()
    wiki_grade = sheet_name[0]
    wiki_year = '2016'
    wiki_class_name = wiki_year + 'kyc' + wiki_grade + wiki_class + 'ind'
    sheet = workbook.sheet_by_name(sheet_name)
    for row_num in range(1, sheet.nrows):
        one_row = sheet.row_values(row_num)
        wiki_group_no = str(one_row[1])[:-2]
        wiki_url = "http://" + one_row[8]
        wiki_admin_key = one_row[10]
        wiki_id = '2016kyc' + wiki_grade + wiki_class + 'indgp' + wiki_group_no
        insert_into_wiki_table(wiki_id, wiki_url, wiki_year, 'kyc', wiki_grade, wiki_class, wiki_group_no, wiki_class_name, wiki_admin_key)


if __name__ == "__main__":
    file_name = 'ExData_kyc.py.xlsx'
    wb = xlrd.open_workbook(file_name)
    # print(wb.sheet_names())
    sh_names = ['4Y', '4K', '4S', '4J']
    for name in sh_names:
        extract_data_from_excel_by_sheet_name(wb, name)

