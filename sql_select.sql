Declare @Dela as date='2018-01-01'
Declare @PanaLA as date='2023-06-30'
Declare @PromCode as varchar(20)='2023LichidareStoc'
select lp.ITEMID ItemId, s.ClientId, Valoare, NrDoc, Qty
from (select xaig.itemid 
			from XMI_AdditionalItemGroups xaig 
			where  xaig.DATAAREAID='MIG'
				and xaig.AdditionalItemGroupID=@PromCode
			) lp
left join (select s.INVOICEACCOUNT ClientId, s.ItemId, sum(s.LINEAMOUNTMST) Valoare, count(s.InvoiceID) NrDoc, sum(s.QTY) Qty
			from OLAP_DAXELES.dbo.Sales s
				join XMI_AdditionalItemGroups xaig on xaig.ITEMID=s.ItemID
									and xaig.DATAAREAID='MIG'
									and xaig.AdditionalItemGroupID=@PromCode 
				join ( select distinct [Cod client]
			from DaxAdditional.tk.ListaEmailDist ls ) ls on ls.[Cod client]=s.INVOICEACCOUNT
			where s.INVOICEDATE between @Dela and @PanaLA
			group by s.INVOICEACCOUNT, s.ItemID ) s on s.ItemID=lp.ITEMID

order by lp.ITEMID
;
Declare @Dela as date='2018-01-01'
Declare @PanaLA as date='2023-06-30'
Declare @PromCode as varchar(20)='2023LichidareStoc'
select lp.ITEMID ItemId
	, i.Sintetic
	,s.ClientId, Valoare, NrDoc, Qty
	from (select xaig.itemid 
				from XMI_AdditionalItemGroups xaig 
				where  xaig.DATAAREAID='MIG'
					and xaig.AdditionalItemGroupID=@PromCode
				) lp
	left join OLAP_DAXELES.dbo.InventTable i on i.ITEMID=lp.ITEMID
	left join (
				select s.INVOICEACCOUNT ClientId, it.Sintetic, sum(s.LINEAMOUNTMST) Valoare, count(s.InvoiceID) NrDoc, sum(s.QTY) Qty
				from OLAP_DAXELES.dbo.Sales s
					left join OLAP_DAXELES.dbo.InventTable it on s.ItemID=it.ItemID
					join ( select distinct [Cod client]
							from DaxAdditional.tk.ListaEmailDist ls ) ls on ls.[Cod client]=s.INVOICEACCOUNT
				where s.INVOICEDATE between @Dela and @PanaLA
				group by s.INVOICEACCOUNT, it.Sintetic
				) s on s.Sintetic=i.Sintetic


order by lp.ITEMID