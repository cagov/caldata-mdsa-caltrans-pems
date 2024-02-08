{% macro map_county_fips_to_county_name(county, k, v) -%}

{#
    County Fips Codes source: https://www2.census.gov/geo/docs/reference/codes2020/cou/st06_ca_cou2020.txt
#}

{% set county_dict = {
    "1" : "Alameda",
    "3" : "Alpine",
    "5" : "Amador",
    "7" : "Butte",
    "9" : "Calaveras",
    "11" : "Colusa",
    "13" : "Contra Costa",
    "15" : "Del Norte",
    "17" : "El Dorado",
    "19" : "Fresno",
    "21" : "Glenn",
    "23" : "Humboldt",
    "25" : "Imperial",
    "27" : "Inyo",
    "29" : "Kern",
    "31" : "Kings",
    "33" : "Lake",
    "35" : "Lassen",
    "37" : "Los Angeles",
    "39" : "Madera",
    "41" : "Marin",
    "43" : "Mariposa",
    "45" : "Mendocino",
    "47" : "Merced",
    "49" : "Modoc",
    "51" : "Mono",
    "53" : "Monterey",
    "55" : "Napa",
    "57" : "Nevada",
    "59" : "Orange",
    "61" : "Placer",
    "63" : "Plumas",
    "65" : "Riverside",
    "67" : "Sacramento",
    "69" : "San Benito",
    "71" : "San Bernardino",
    "73" : "San Diego",
    "75" : "San Francisco",
    "77" : "San Joaquin",
    "79" : "San Luis Obispo",
    "81" : "San Mateo",
    "83" : "Santa Barbara",
    "85" : "Santa Clara",
    "87" : "Santa Cruz",
    "89" : "Shasta",
    "91" : "Sierra",
    "93" : "Siskiyou",
    "95" : "Solano",
    "97" : "Sonoma",
    "99" : "Stanislaus",
    "101" : "Sutter",
    "103" : "Tehama",
    "105" : "Trinity",
    "107" : "Tulare",
    "109" : "Tuolumne",
    "111" : "Ventura",
    "113" : "Yolo",
    "115" : "Yuba"
} -%}




case
    {% for k, v in county_dict.items() -%}
    when "{{ county }}" = '{{ k }}'
    then '{{ v }}'
    {% endfor -%}
end

{%- endmacro %}