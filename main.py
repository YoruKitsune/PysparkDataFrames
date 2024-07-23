from data import spark, categories, products, adapter

# метод печати всех продуктов 
def PrintAllProducts():
  products.join(adapter, products.id == adapter.id_prod, "left").join(categories, adapter.id_cat == categories.id,'left').select(products.product, categories.category).sort(products.product.desc()).show()
  
PrintAllProducts()