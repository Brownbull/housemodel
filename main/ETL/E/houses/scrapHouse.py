class ScrapHouse:
  def __init__(self, default=-16):
    self.PriceUnit = default
    self.Price = default
    self.item = default
    self.Link = default
    self.fld0 = default
    self.fld1 = default
    self.fld2 = default
    self.fld3 = default
    self.fld4 = default
    self.fld5 = default
    self.Description = default
    self.PropertyState = default
    self.PublishedDate = default
    self.Title = default
    self.Section = default
    self.Models = default

  def toCsvRowScrapy(self):
    return "\"{}\",\"{}\",\"{}\",\"{}\"\n".format(
      self.PriceUnit,
      self.Price,
      self.item,
      self.Link)

  def toCsvRow(self):
    return "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n".format(
      self.PriceUnit,
      self.Price,
      self.item,
      self.Link,
      self.fld0,
      self.fld1,
      self.fld2,
      self.fld3,
      self.fld4,
      self.fld5,
      self.Description,
      self.PropertyState,
      self.PublishedDate,
      self.Title,
      self.Section,
      self.Models)

  def setPortalFields(self, PriceUnit , Price , item , Link):
    self.PriceUnit = PriceUnit
    self.Price = Price
    self.item = item
    self.Link = Link
  
  def setItemFields(self, fld0, fld1, fld2, fld3, fld4, fld5, Description, PropertyState, PublishedDate, Title, Section, Models):
    self.fld0 = fld0
    self.fld1 = fld1
    self.fld2 = fld2
    self.fld3 = fld3
    self.fld4 = fld4
    self.fld5 = fld5
    self.Description = Description
    self.PropertyState = PropertyState
    self.PublishedDate = PublishedDate
    self.Title = Title
    self.Section = Section
    self.Models = Models
    