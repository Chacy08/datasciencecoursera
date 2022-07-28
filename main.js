$(function() {
//overriding jquery-ui.autocomplete .js functions
$.ui.autocomplete.prototype._renderMenu = function(ul, items) {
  var self = this;
  //table definitions
  ul.append("<table><thead><tr><th>Item Code</th><th>Name</th><th>Item Brand</th><th>Material Brand</th><th>UOM</th></tr></thead><tbody></tbody></table>");
  $.each( items, function( index, item ) {
    self._renderItemData(ul, ul.find("table tbody"), item );
  });
};
$.ui.autocomplete.prototype._renderItemData = function(ul,table, item) {
  return this._renderItem( table, item ).data( "ui-autocomplete-item", item );
};
$.ui.autocomplete.prototype._renderItem = function(table, item) {
  return $( "<tr class='ui-menu-item' role='presentation'></tr>" )
    //.data( "item.autocomplete", item )
    .append( "<td >"+item.item_code+"</td>"+"<td>"+item.item_desc+"</td>"+"<td>"+item.item_brand+"</td>" )
    .appendTo( table );
};
//random json values

var projects;
$.getJSON( "https://github.com/Chacy08/datasciencecoursera/blob/master/items.json", function( projects ) {
	console.log("json loaded");
 });
$( "#project" ).autocomplete({
	minLength: 1,
	source: projects,
    
	focus: function( event, ui ) {
		console.log(ui.item.item_desc);
        $( "#project" ).val( ui.item.item_desc );
		$( "#project-id" ).val( ui.item.item_code );
		$( "#project-description" ).html( ui.item.item_brand );
		return false;
	}//you can write for select too
    /*select:*/
})
});