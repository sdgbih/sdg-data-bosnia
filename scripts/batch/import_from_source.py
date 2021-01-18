import os
import glob
import pandas as pd
import numpy as np
import yaml
import yamlmd
import sdg

translations = sdg.translations.TranslationInputSdmx(source='https://registry.sdmx.org/ws/public/sdmxapi/rest/datastructure/IAEG-SDGs/SDG/latest/?format=sdmx-2.1&detail=full&references=children')
translations.execute()
english = translations.get_translations()['en']

sdmx_compatibility = True

path = 'SDG_Indicators_Global_BIH_oct_2020_EN.xls'
start_cols = [
    'SDG target',
    'SDG indicator',
    'Series',
    'Unit',
]
end_cols = [
    'Comments',
    'Sources',
    'Links',
    'Custodian agency',
    'Link to the global metadata (1) of this indicator:',
    'Link to the global metadata (2) of this indicator:',
]

# Hardcoded some details about the source data, to keep this script simple.
sheet_info = {
    'SDG 1': {
        'goal': 1,
        'disaggregations': ['Location','Age','Reporting Type','Sex'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 2': {
        'goal': 2,
        'disaggregations': ['Reporting Type','Age','Sex','Type of product'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 3': {
        'goal': 3,
        'disaggregations': ['Reporting Type','Age','Sex','Name of non-communicable disease','Type of occupation', 'IHR Capacity'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 4': {
        'goal': 4,
        'disaggregations': ['Reporting Type','Education level','Quantile','Sex','Type of skill','Location'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 5': {
        'goal': 5,
        'disaggregations': ['Reporting Type','Age','Sex'],
        'year_start': 2000,
        'year_end': 2020,
    },
    'SDG 6': {
        'goal': 6,
        'disaggregations': ['Reporting Type','Location'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 7': {
        'goal': 7,
        'disaggregations': ['Reporting Type','Location'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 8': {
        'goal': 8,
        'disaggregations': ['Reporting Type','Activity','Sex','Age','Type of product'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 9': {
        'goal': 9,
        'disaggregations': ['Reporting Type','Mode of transportation'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 10': {
        'goal': 10,
        'disaggregations': ['Reporting Type','Name of international institution','Type of product'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 11': {
        'goal': 11,
        'disaggregations': ['Reporting Type','Location'],
        'year_start': 2000,
        'year_end': 2020,
    },
    'SDG 12': {
        'goal': 12,
        'disaggregations': ['Reporting Type','Type of product'],
        'year_start': 2000,
        'year_end': 2020,
    },
    'SDG 13': {
        'goal': 13,
        'disaggregations': ['Reporting Type'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 14': {
        'goal': 14,
        'disaggregations': ['Reporting Type'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 15': {
        'goal': 15,
        'disaggregations': ['Reporting Type','Level/Status'],
        'year_start': 2000,
        'year_end': 2020,
    },
    'SDG 16': {
        'goal': 16,
        'disaggregations': ['Reporting Type','Sex','Age','Parliamentary committees','Name of international institution'],
        'year_start': 2000,
        'year_end': 2020,
    },
    'SDG 17': {
        'goal': 17,
        'disaggregations': ['Reporting Type','Type of speed','Type of product'],
        'year_start': 2000,
        'year_end': 2019,
    },
}

things_to_translate = {}

strip_from_values = ['<', 'NaN', 'NA', 'fn', 'C', 'A', 'E', 'G', 'M', 'N', ',']
def clean_data_value(value):
    if value == '-':
        return pd.NA
    for strip_from_value in strip_from_values:
        value = value.replace(strip_from_value, '')
    value = value.strip()
    if value == '':
        return pd.NA
    test = float(value)
    return value


def drop_these_columns():
    # These columns aren't useful for some reason.
    return [
        # This only had 1 value in the source data.
        'Reporting Type',
        # This only had 1 value in the source data.
        'Level/Status',
        # These are in the metadata.
        'SDG target',
        'SDG indicator',
    ]


def convert_composite_breakdown_label(label):
    return label.replace(' ', '_').replace('-', '_').lower()


def translate(group, key):
    translated = key
    if group in english and key in english[group]:
        translated = english[group][key]
    return translated


def get_column_name_changes():
    changes = {
        # These serve specific purposes in Open SDG.
        'Unit': 'UNIT_MEASURE',
        'Series': 'SERIES',
        # These changes are for compatibility with SDMX.
    }
    sdmx_changes = {
        'Sex': 'SEX',
        'Age': 'AGE',
        'Location': 'URBANISATION',
        'Quantile': 'INCOME_WEALTH_QUANTILE',
        'Education level': 'EDUCATION_LEV',
        'Activity': 'ACTIVITY',
        'IHR Capacity': 'COMPOSITE_BREAKDOWN',
        'Mode of transportation': 'COMPOSITE_BREAKDOWN',
        'Name of international institution': 'COMPOSITE_BREAKDOWN',
        'Name of non-communicable disease': 'COMPOSITE_BREAKDOWN',
        'Type of occupation': 'OCCUPATION',
        'Type of product': 'PRODUCT',
        'Type of skill': 'COMPOSITE_BREAKDOWN',
        'Type of speed': 'COMPOSITE_BREAKDOWN',
        'Parliamentary committees': 'COMPOSITE_BREAKDOWN',
        'Reporting Type': 'Reporting Type',
        'Level/Status': 'Level/Status',
    }
    if sdmx_compatibility:
        changes.update(sdmx_changes)

    for key in changes:
        changed = changes[key]
        if changed not in things_to_translate:
            things_to_translate[changed] = {}
        if changed == 'COMPOSITE_BREAKDOWN':
            comp_breakdown_label = convert_composite_breakdown_label(key)
            things_to_translate[changed][comp_breakdown_label] = key
        else:
            things_to_translate[changed][changed] = translate(changed, changed)
    return changes
# run it right away
get_column_name_changes()

def clean_disaggregation_value(value, column=''):
    if pd.isna(value):
        return ''
    if value.strip() == '':
        return ''
    fixed = value
    conversions = {}
    if column == 'Age':
        conversions = {
            'ALL': 'ALL AGE',
            '<5y': '<5Y',
        }
    if sdmx_compatibility:
        if column == 'Location':
            conversions = {
                'ALL AREA': '_T',
                'RURAL': 'R',
                'URBAN': 'U',
            }
        if column == 'Age':
            conversions = {
                'ALL': '_T', # Instead of _T
                'ALL AGE': '_T',
                '15-19': 'Y15T19',
                '15-24': 'Y15T24',
                '15-25': 'Y15T25', # custom
                '15-26': 'Y15T26', # custom
                '15-49': 'Y15T49',
                '15+': 'Y_GE15',
                '18+': 'Y_GE18',
                '<18Y': 'Y0T17',
                '<1M': 'M0',
                '<1Y': 'Y0',
                '20-24': 'Y20T24',
                '25+': 'Y_GE25',
                '30-70': 'Y30T70',
                '<5Y': 'Y0T4',
                '<5y': 'Y0T4',
                '46+': 'Y_GE46',
                '2-14': 'Y2T14',
            }
        if column == 'Sex':
            conversions = {
                'FEMALE': 'F',
                'MALE': 'M',
                'BOTHSEX': '_T',
            }
        if column == 'Mode of transportation':
            conversions = {
                'RAI': 'MOT_RAI',
                'ROA': 'MOT_ROA',
                'IWW': 'MOT_IWW',
                'SEA': 'MOT_SEA',
            }
        if column == 'Name of international institution':
            conversions = {
                'ECOSOC': 'IO_ECOSOC',
                'IBRD': 'IO_IBRD',
                'IFC': 'IO_IFC',
                'IMF': 'IO_IMF',
                'UNGA': 'IO_UNGA',
                'UNSC': 'IO_UNSC',
            }
        if column == 'Name of non-communicable disease':
            conversions = {
                'CAN': 'NCD_CNCR',
                'CAR': 'NCD_CARDIO',
                'RES': 'NCD_CRESPD',
                'DIA': 'NCD_DIABTS',
            }
        if column == 'IHR Capacity':
            conversions = {
                'IHR01': 'IHR_01',
                'IHR02': 'IHR_02',
                'IHR03': 'IHR_03',
                'IHR04': 'IHR_04',
                'IHR05': 'IHR_05',
                'IHR06': 'IHR_06',
                'IHR07': 'IHR_07',
                'IHR08': 'IHR_08',
                'IHR09': 'IHR_09',
                'IHR10': 'IHR_10',
                'IHR11': 'IHR_11',
                'IHR12': 'IHR_12',
                'IHR13': 'IHR_13',
                'SPAR01': 'SPAR_01',
                'SPAR02': 'SPAR_02',
                'SPAR03': 'SPAR_03',
                'SPAR04': 'SPAR_04',
                'SPAR05': 'SPAR_05',
                'SPAR06': 'SPAR_06',
                'SPAR07': 'SPAR_07',
                'SPAR08': 'SPAR_08',
                'SPAR09': 'SPAR_09',
                'SPAR10': 'SPAR_10',
                'SPAR11': 'SPAR_11',
                'SPAR12': 'SPAR_12',
                'SPAR13': 'SPAR_13',
            }
        if column == 'Quantile':
            conversions = {
                '_T': '_T',
            }
        if column == 'Type of occupation':
            conversions = {
                'DENT': 'ISCO08_2261',
                'NURS': 'ISCO08_2221_3221',
                'NURSMID': 'ISCO08_222_322',
                'PHAR': 'ISCO08_2262',
                'PHYS': 'ISCO08_221',
            }
        if column == 'Type of product':
            conversions = {
                'AGR': 'AGG_AGR',
                'ALP': '_T',
                'ARM': 'AGG_ARMS',
                'BIM': 'MF1',
                'CLO': 'AGG_CLTH',
                'COL': 'MF41',
                'CPR': 'MF121',
                'CRO': 'MF11',
                'FEO': 'MF21',
                'FOF': 'MF4',
                'GAS': 'MF422',
                'GBO': 'MF122',
                'IND': 'AGG_IND',
                'MEO': 'MF2',
                'NFO': 'MF22',
                'NMA': 'MF_AGG3B',
                'NMC': 'MF_AGG3A',
                'NMM': 'MF3',
                'OIL': 'AGG_OIL',
                'PET': 'MF421',
                'TEX': 'AGG_TXT',
                'WCH': 'MF14',
                'WOD': 'MF13',
                'MAZ': 'CPC2_1_112',
                'RIC': 'CPC2_1_113',
                'SOR': 'CPC2_1_114',
                'WHE': 'CPC2_1_111',
            }
        if column == 'Education level':
            conversions = {
                'LOWSEC': 'ISCED11_2',
                'PRIMAR': 'ISCED11_1',
                'UPPSEC': 'ISCED11_3',
            }
        if column == 'Type of skill':
            conversions = {
                'SKILL MATH': 'SKILL_MATH',
                'SKILL READ': 'SKILL_READ',
                'SOFT': 'SKILL_ICTSFWR',
                'TRAF': 'SKILL_ICTTRFF',
                'CMFL': 'SKILL_ICTCMFL',
                'PCPR': 'SKILL_ICTPRGM',
                'EPRS': 'SKILL_ICTPST',
                'EMAIL': 'SKILL_ICTATCH',
                'COPA': 'SKILL_ICTCPT',
                'ARSP': 'SKILL_ICTSSHT',
            }
        if column == 'Type of speed':
            conversions = {
                '256KT2MBPS': 'IS_256KT2M',
                '2MT10MBPS': 'IS_2MT10M',
                '10MBPS': 'IS_GE10M',
                'ANYS': '_T',
            }
        if column == 'Activity':
            conversions = {
                'ISIC4_A': 'ISIC4_A',
                'NONAGR': 'ISIC4_BTU',
                'TOTAL': '_T',
            }
        if column == 'Parliamentary committees':
            conversions = {
                'FOR_AFF': 'PC_FOR_AFF',
                'DEFENCE': 'PC_DEFENCE',
                'FINANCE': 'PC_FINANCE',
                'HUM_RIGH': 'PC_HUM_RIGH',
                'GEN_EQU': 'PC_GEN_EQU',
            }
    if value in conversions:
        fixed = conversions[value]
    fixed_column = get_column_name_changes()[column]
    if fixed_column not in things_to_translate:
        things_to_translate[fixed_column] = {}
    things_to_translate[fixed_column][fixed] = translate(fixed_column, fixed)
    return fixed


def clean_metadata_value(column, value):
    if pd.isna(value):
        return ''
    return value.strip()


def convert_metadata_column(column):
    conversions = {
        'Comments': 'comments',
        'Sources': 'source_organisation_1',
        'Links': 'source_url_1',
        'Custodian agency': 'un_custodian_agency',
        'Link to the global metadata (1) of this indicator:': 'goal_meta_link',
        'Link to the global metadata (2) of this indicator:': 'goal_meta_link_2',
    }
    return conversions[column]


def get_indicator_id(indicator_name):
    return indicator_name.strip().split(' ')[0]


def get_indicator_name(indicator_name):
    return ' '.join(indicator_name.strip().split(' ')[1:])


def clean_series(series):
    fixed = series.strip()
    # Weird space character.
    fixed = fixed.replace(' ', ' ')
    # Some have line breaks.
    if '\n' in fixed:
        fixed = fixed.split('\n')[-1]
    # Return the last word.
    fixed = fixed.split(' ')[-1]
    fixed = fixed.upper()
    # From this point, perform a few manual fixes.
    conversions = {
        'IT_NET_BB': 'IT_NET_BBND',
        'IT_NET_BBN': 'IT_NET_BBNDN',
        'AG_PRD_FIESMSI': 'AG_PRD_FIESMS',
        'AG_PRD_FIESMSIN': 'AG_PRD_FIESMSN',
        'AG_PRD_FIESSI': 'AG_PRD_FIESS',
        'AG_PRD_FIESSIN': 'AG_PRD_FIESSN',
        'EG_ELC_ACCS': 'EG_ACS_ELEC',
        'ER_PTD_FRWRT': 'ER_PTD_FRHWTR',
        'ER_PTD_MOTN': 'ER_PTD_MTN',
        'ER_PTD_TERRS': 'ER_PTD_TERR',
        'ER_RSK_LSTI': 'ER_RSK_LST',
        'SH_DTH_RNCOM': 'SH_DTH_NCD',
        'SH_MED_HEAWOR': 'SH_MED_DEN',
        'SH_STA_MMR': 'SH_STA_MORT',
        'SH_STA_OVRWGT': 'SN_STA_OVWGT',
        'SH_STA_OVRWGTN': 'SN_STA_OVWGTN',
        'SH_STA_STUNT': 'SH_STA_STNT',
        'SH_STA_STUNTN': 'SH_STA_STNTN',
        'SH_STA_WASTE': 'SH_STA_WAST',
        'SH_STA_WASTEN': 'SH_STA_WASTN',
        'SH_TBS_INCID': 'SH_TBS_INCD',
        'VC_PRS_UNSEC': 'VC_PRS_UNSNT',
    }
    if fixed in conversions:
        fixed = conversions[fixed]

    if fixed.strip() != '':
        things_to_translate['SERIES'][fixed] = series
    return fixed


def clean_unit(unit):
    if pd.isna(unit) or unit == '':
        return ''
    fixes = {}
    sdmx_fixes = {
        '% (PERCENT)': 'PT',
        '$ (USD)': 'USD',
        'MILIONS': 'MILLIONS', # custom
        'THOUSANDS': 'THOUSANDS', # custom
        'INDEX': 'IX',
        'PER 100000 LIVE BIRTHS': 'PER_100000_LIVE_BIRTHS',
        'PER 1000 LIVE BIRTHS': 'PER_1000_LIVE_BIRTHS',
        'PER 1000 UNINFECTED POPULATION': 'PER_1000_UNINFECTED_POP',
        'PER 100000 POPULATION': 'PER_100000_POP',
        'PER 1000  POPULATION': 'PER_1000_POP',
        'LITRES': 'LITRES', # custom
        'PER 1000 POPULATION': 'PER_1000_POP',
        "'PER 10000 POPULATION": 'PER_10000_POP',
        'RATIO': 'RO',
        'SCORE': 'SCORE',
        'USD/m3': 'USD_PER_M3',
        'KMSQ': 'KM2',
        'M M3 PER ANNUM': 'M_M3_PER_YR',
        'MJPER GDP CON PPP USD': 'MJ_PER_GDP_CON_PPP_USD',
        'W PER CAPITA': 'W_PER_CAPITA',
        'TONNES': 'TONNES', # custom
        'KG PER CON USD': 'KG_PER_CON_USD',
        'CUR LCU': 'CUR_LCU',
        'PER 100000 EMPLOYEES': 'PER_100000_EMP',
        'CON USD': 'CON_USD',
        '%': 'PT',
        'METONS': 'T',
        'T KM': 'T_KM',
        'P KM': 'P_KM',
        'TONNES M': 'TONNES_M', # custom
        'PER 1000000 POPULATION': 'PER_1000000_POP',
        'mgr/m^3': 'GPERM3',
        'CU USD B': 'USD_B', # custom
        'HA TH': 'HA_TH', # custom
        'CUR LCU M': 'CUR_LCU_M', # custom
        'PER 100 POPULATION': 'PER_100_POP',
        'CU USD': 'USD',
    }
    if sdmx_compatibility:
        fixes.update(sdmx_fixes)
    fixed = unit
    if unit in fixes:
        fixed = fixes[unit]
    if 'UNIT_MEASURE' not in things_to_translate:
        things_to_translate['UNIT_MEASURE'] = {}
    things_to_translate['UNIT_MEASURE'][fixed] = translate('UNIT_MEASURE', fixed)
    return fixed


data = {}
metadata = {}

for sheet in sheet_info:
    print('Processing sheet: ' + sheet)
    info = sheet_info[sheet]
    year_cols = [str(year) for year in range(info['year_start'], info['year_end'] + 1)]
    columns = start_cols + info['disaggregations'] + year_cols + end_cols
    converters = { year: clean_data_value for year in year_cols }
    df = pd.read_excel(path,
        sheet_name=sheet,
        usecols=columns,
        names=columns,
        skiprows=[0, 1],
        na_values=['-'],
        converters = converters
    )
    for col in info['disaggregations']:
        df[col] = df[col].apply(clean_disaggregation_value, column=col)
    # Fill in the merged cells.
    df['SDG target'] = df['SDG target'].fillna(method='ffill')
    df['SDG indicator'] = df['SDG indicator'].fillna(method='ffill')
    df['Series'] = df['Series'].fillna(method='ffill')

    # Drop rows without data.
    df = df.dropna(subset=year_cols, how='all')
    # Convert units and series.
    df['Series'] = df['Series'].apply(clean_series)
    df['Unit'] = df['Unit'].apply(clean_unit)

    for index, row in df.iterrows():

        # Convert the data.
        data_df = pd.melt(row.to_frame().transpose(),
            id_vars=start_cols + info['disaggregations'],
            value_vars=year_cols,
            var_name='Year',
            value_name='Value'
        )

        indicator_id = get_indicator_id(row['SDG indicator'])
        if indicator_id not in data:
            data[indicator_id] = data_df
            metadata[indicator_id] = {}
        else:
            data[indicator_id] = pd.concat([data[indicator_id], data_df])

        # Convert the metadata.
        for col in end_cols:
            if pd.isna(row[col]):
                continue
            if col not in metadata[indicator_id]:
                value = clean_metadata_value(col, row[col])
                key = convert_metadata_column(col)
                metadata[indicator_id][key] = value
        # Add a few more metadata values.
        metadata[indicator_id]['sdg_goal'] = str(info['goal'])
        metadata[indicator_id]['reporting_status'] = 'complete'
        metadata[indicator_id]['indicator_number'] = indicator_id
        if 'source_organisation_1' in metadata[indicator_id]:
            metadata[indicator_id]['source_active_1'] = True
        # Set up dynamic graph titles by series.
        if 'graph_titles' not in metadata[indicator_id]:
            metadata[indicator_id]['graph_titles'] = {}
        metadata[indicator_id]['graph_titles'][row['Series']] = True

        column_name_changes = get_column_name_changes()
        for column in info['disaggregations']:
            if column_name_changes[column] == 'COMPOSITE_BREAKDOWN':
                metadata[indicator_id]['composite_breakdown_label'] = 'COMPOSITE_BREAKDOWN.' + convert_composite_breakdown_label(column)
                break

for indicator_id in data:
    slug = indicator_id.replace('.', '-')
    data_path = os.path.join('data', 'indicator_' + slug + '.csv')
    df = data[indicator_id]
    for column in drop_these_columns():
        if column in df.columns:
            df = df.drop(columns=[column])
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.dropna(subset=['Value'], how='all')
    df = df.dropna(axis='columns', how='all')
    df = df.rename(columns=get_column_name_changes())
    non_value_columns = df.columns.tolist()
    non_value_columns.pop(non_value_columns.index('Value'))
    df = df.drop_duplicates(subset=non_value_columns)

    # Rearrange the columns.
    cols = df.columns.tolist()
    cols.pop(cols.index('Year'))
    cols.pop(cols.index('Value'))
    cols = ['Year'] + cols + ['Value']
    df = df[cols]

    df.to_csv(data_path, index=False)

    # Fix the special "graph_titles" metadata field we added above.
    graph_titles = []
    for series in metadata[indicator_id]['graph_titles']:
        graph_titles.append({
            'series': 'SERIES.' + series,
            'title': 'SERIES.' + series,
        })
    metadata[indicator_id]['graph_titles'] = graph_titles

    meta_path = os.path.join('meta', slug + '.md')
    meta = yamlmd.read_yamlmd(meta_path)
    for field in metadata[indicator_id]:
        meta[0][field] = metadata[indicator_id][field]
    yamlmd.write_yamlmd(meta, meta_path)

skip_translations = drop_these_columns()
for key in things_to_translate:
    if key in skip_translations:
        continue
    if key not in english:
        english[key] = things_to_translate[key]
    else:
        for item in things_to_translate[key]:
            if item not in english[key]:
                english[key][item] = things_to_translate[key][item]

skip_translations = [
    'BASE_PER',
    'COMMENT_OBS',
    'COMMENT_TS',
    'CUST_BREAKDOWN',
    'CUST_BREAKDOWN_LB',
    'DATA_LAST_UPDATE',
    'GEO_INFO_TYPE',
    'GEO_INFO_URL',
    'LOWER_BOUND',
    'NATURE',
    'OBS_STATUS',
    'REF_AREA',
    'REPORTING_TYPE',
    'SOURCE_DETAIL',
    'TIME_COVERAGE',
    'TIME_DETAIL',
    'UNIT_MULT',
    'UPPER_BOUND',
    'FREQ',
]
def convert_translated_text(group, key, text):
    # Remove the bracketed indicator number from SERIES codes
    if group == 'SERIES':
        first_bracket = text.find('[')
        if first_bracket > -1:
            text = text[0:first_bracket].strip()

    if group == 'COMPOSITE_BREAKDOWN':
        text = text.strip().split(': ')[-1]

    if group == 'PRODUCT':
        text = text.replace(' (material flows)', '')
    return text

for group in english:
    add_to_end = {}
    if group in skip_translations:
        continue
    for key in english[group]:
        english[group][key] = convert_translated_text(group, key, english[group][key])
        if group not in things_to_translate or key not in things_to_translate[group]:
            add_to_end[key] = english[group][key]
    for key in add_to_end:
        del english[group][key]

    if english[group]:
        filepath = os.path.join('translations', 'en', group + '.yml')
        with open(filepath, 'w') as file:
            yaml.dump(english[group], file)

    if add_to_end:
        filepath = os.path.join('translations-unused', 'en', group + '.yml')
        with open(filepath, 'w') as file:
            yaml.dump(add_to_end, file)
