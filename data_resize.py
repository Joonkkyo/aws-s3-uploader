from PIL import Image

img = Image.open('./data_origin/origin.jpg')

# 300K
img_s = img.resize((5*512, 4*512))
img_s.save('./data/img_s.jpg')

# 3M
img_m = img.resize((7*1024, 7*1024))
img_m.save('./data/img_m.jpg')

# 300M
img_l = img.resize((10*1000, 10*1000))
img_l.save('./data/img_l.tiff')
