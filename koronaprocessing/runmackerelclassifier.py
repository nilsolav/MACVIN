import KoronaScript as ks
import KoronaScript.Modules as ksm

# For testing
input_dir = '/data/cruise_data/2012/S2012843_PBRENNHOLM_4405/EK60Raw'
output_dir = '/data/s3/MACWIN-scratch/2012/S2012843_PBRENNHOLM_4405/ACOUSTIC/LSSS/KORONA/'

ks = ks.KoronaScript(Categorization='categorization.xml',
                     HorizontalTransducerOffsets='HorizontalTransducerOffsets.xml')

ks.add(ksm.EmptyPingRemoval())
ks.add(ksm.Categorization())
ks.write()
ks.run(src=input_dir, dst=output_dir)

