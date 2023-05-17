# Selection and validation of Rare Diseases with ICD-10/SNOMED-CT mappings

## 1. Orphanet Rare Diseases selection for analysis

https://colab.research.google.com/drive/14gYzMLM4SNMn4PA9SjoEtg1dNvgaWGco?usp=sharing

The above Google colab notebook is to get a computable phenotype definitions for identifying rare diseases reliablly so that we can analyse RD patients' adverse events related to COVID-19 in England.

Technically, this includes:

identify rare diseases from Orphadata for sensible COVID-19 analysis - defined as those for which the point prevalence is applicable.
map Orphadata RDs to codes (ICD-10 and SNOMED-CT) that are used in the England health systems

## 2. Populate human validated ICD-10/SNOEMD-CT mappings for the Trusted Research Envrionment

https://colab.research.google.com/drive/1xdWxNxHCnisinlizy68bT1cC1xObyIMi?usp=sharing

The above colab notebook is for (1) filterting out the Oprhanet data with human-expert validated mappings and (2) output the mappings in a format that we can utlise in the Trusted Research Environment.
