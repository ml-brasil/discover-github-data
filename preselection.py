import octograb
import octograb.preselection

octograb.configure('config.json')

# Get all archives from github and convert to small CSV files, grouped by day
octograb.preselection.convert_archives()

# Merge all CSV files and create a single CSV dataset
octograb.preselection.merge_archives()

# Grav the large single dataset and split them into small sets to be crawled
octograb.preselection.export_inputs()