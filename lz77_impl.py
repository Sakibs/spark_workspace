def lz77_compress(arr): 
	compress = []

	w_start = 0
	i = 0
	while i < len(arr):
		# print '*****'+str(i)
		back = i-1
		moveback = 0
		newi = 0

		# search for longest repeating subsequence
		mx = 0
		mx_ix = 0

		while back >= 0:
			if(arr[back] == arr[i]):
				# print '-- checking'
				check = 0

				while i + check < len(arr) and arr[back+check] == arr[i+check]:
					# print ('-- ', arr[back+check], arr[i+check], back, check)
					check += 1

				if check > mx:
					mx_ix = back
					mx = check
			back -= 1

		moveback = i-mx_ix
		newi = i+mx

		i = newi + 1
		if i <= len(arr):
			compress.append((mx_ix, mx, arr[i-1]))
			print (mx_ix, mx, arr[i-1])
		else:
			compress.append((mx_ix, mx, '$'))
			print (mx_ix, mx, '$')

	return compress

def lz77_decompress(compressed):
	s = ''
	for item in compressed:
		start = item[0]
		count = item[1]
		next_char = item[2]

		i=0
		while i<count:
			s += s[start+i]
			i+=1

		if next_char != '$':
			s += next_char

	return s


if __name__ == "__main__":
	rep = lz77_compress("PPUMMMMMMMMMM")
	print lz77_decompress(rep)
	print '=============================='
	rep = lz77_compress("abcdecdecdecde")
	print lz77_decompress(rep)
	print '=============================='
	rep = lz77_compress("abcdecdecdecdefgfgfgh")
	print lz77_decompress(rep)
	print '=============================='
	rep = lz77_compress("aacaacabcabaaac")
	print lz77_decompress(rep)