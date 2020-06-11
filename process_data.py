import h5py, os, psycopg2, sys, math
from multiprocessing import Process
from xml.dom import minidom

ORBIT_DIRECTION_GCO_INDEX = 14 # The 14th gco:CharacterString element in the *.iso.xml file contains the relevant orbit direction information

PROCESS_COUNT = 24 # How many processes you want

# The bounds of the subset of data you want
S_BOUND = -0.5
N_BOUND = -0.25
E_BOUND = -80.25
W_BOUND = -80.5

# Modified binary search 
def find_asc_index(data, low_index, high_index, value) : 
	if data[low_index] >= value:
		return low_index

	if high_index >= low_index: 
		mid_index = low_index + (high_index - low_index) // 2
		if( mid_index == 0 or value > data[mid_index - 1] ) and data[mid_index] >= value : 
			return mid_index 
		elif value > data[mid_index] : 
			if low_index == high_index:
				return high_index
			return find_asc_index(data, mid_index + 1, high_index, value) 
		else : 
			return find_asc_index(data, low_index, mid_index - 1, value) 
	return -1

def find_desc_index(data, low_index, high_index, value):
	if data[low_index] <= value:
		return low_index

	if high_index >= low_index: 
		mid_index = low_index + (high_index - low_index) // 2
		if ( mid_index == 0 or value < data[mid_index - 1] ) and data[mid_index] <= value : 
			return mid_index 
		elif value < data[mid_index]: 
			if low_index == high_index:
				return high_index
			return find_desc_index(data, mid_index + 1, high_index, value) 
		else : 
			return find_desc_index(data, low_index, mid_index - 1, value) 
	return -1

def find_track_bounds(file, track, orbit_dir):
	data = h5py.File(file, 'r')
	track_data = data[track]['heights']
	list_length = len(track_data['h_ph'])

	starting_index, ending_index = 0, 0

	# Redundant but necessary. Slightly different calculations depending on whether ascending or descending
	if orbit_dir == "D":
		southern_bound_index = find_desc_index(track_data['lat_ph'], 0, list_length - 1, S_BOUND)
		northern_bound_index = find_desc_index(track_data['lat_ph'], 0, list_length - 1, N_BOUND)
		eastern_bound_index = find_desc_index(track_data['lon_ph'], 0, list_length - 1, E_BOUND)
		western_bound_index = find_desc_index(track_data['lon_ph'], 0, list_length - 1, W_BOUND)

		starting_index = northern_bound_index if northern_bound_index > eastern_bound_index else eastern_bound_index
		ending_index = southern_bound_index if southern_bound_index < western_bound_index else western_bound_index
	elif orbit_dir == "A":
		southern_bound_index = find_asc_index(track_data['lat_ph'], 0, list_length - 1, S_BOUND)
		northern_bound_index = find_asc_index(track_data['lat_ph'], 0, list_length - 1, N_BOUND)
		eastern_bound_index = find_desc_index(track_data['lon_ph'], 0, list_length - 1, E_BOUND)
		western_bound_index = find_desc_index(track_data['lon_ph'], 0, list_length - 1, W_BOUND)

		starting_index = southern_bound_index if southern_bound_index > eastern_bound_index else eastern_bound_index
		ending_index = northern_bound_index if northern_bound_index < western_bound_index else western_bound_index
	else:
		# I don't think it is possible for this to happen
		print("This shouldn't have gotten this far.")
		print("Unrecognized orbit direction: " + orbit_dir)
		print("Exiting")
		sys.exit()

	# Check to see if we found any viable data in the track. If not, skip it
	if southern_bound_index == northern_bound_index or eastern_bound_index == western_bound_index or ending_index < starting_index:
		print("\tTrack not in bounds. Skipping.")
		return -1, -1

	return starting_index, ending_index

def process_track_segment(file, track_id, starting_index, ending_index):
	data = h5py.File(file, 'r')
	track_data = data[track_id]['heights']
	segment_length = ending_index - starting_index

	#for index in range(starting_index, ending_index):
	#	do something fun here	

	print("\t\tFinished processing segment on job with id: %s" % os.getpid())

def determine_orbit_direction(file):
	data = minidom.parse(file[:-2] + 'iso.xml')

	orbit_data = data.getElementsByTagName('gco:CharacterString')[ORBIT_DIRECTION_GCO_INDEX].firstChild.data
	start_dir = orbit_data.split("StartDirection:")[1].split(" ")[0]
	end_dir = orbit_data.split("EndDirection: ")[1]

	return start_dir, end_dir

def process_file(file):
	data = h5py.File(file, 'r')
	print("Opening file: %s" % file)
	tracks = ['gt1r', 'gt1l', 'gt2r', 'gt2l', 'gt3r', 'gt3l']
	orbit_start_dir, orbit_end_dir = determine_orbit_direction(file) 

	if orbit_start_dir != orbit_end_dir:
		print("This program doesn't handle data sets non matching starting and ending orbit directions. Exiting.")
		sys.exit()

	jobs = []

	for track in tracks:
		if track not in data.keys():
			print("\tCould not find track: %s. Continuing." % track)
			continue

		starting_index, ending_index = find_track_bounds(file, track, orbit_end_dir)
		if starting_index == -1 or ending_index == -1:
			continue

		print("\tProcessing track: %s" % track)
		viable_track_length = ending_index - starting_index
		process_segment_length = math.ceil(viable_track_length / PROCESS_COUNT)

		for thread_index in range(PROCESS_COUNT):
			job_starting_index = 0
			job_ending_index = 0

			if thread_index == 0:
				job_starting_index = starting_index
				job_ending_index = process_segment_length
			elif thread_index == PROCESS_COUNT - 1:
				job_starting_index = thread_index * process_segment_length
				job_ending_index = viable_track_length
			else:
				job_starting_index = thread_index * process_segment_length
				job_ending_index = (thread_index + 1) * process_segment_length		

			p = Process(target=process_track_segment, args=(file, track, job_starting_index, job_ending_index,))
			jobs.append(p)
			p.start()

		for job in jobs:
			job.join()
		print()
	print()

if __name__ == '__main__':
	for directory, subdirectories, files in os.walk('.'):
		for file in files:
			if file.endswith('.h5'):
				process_file(os.path.join(directory, file))

