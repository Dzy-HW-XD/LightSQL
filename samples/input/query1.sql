SELECT  R.G, s.C, s2.A,b0.D, s2.B, b0.E, b0.F, b1.D, b1.E, b1.F,R.H FROM Reserves R,Sailors s,Sailors s2,Boats b0,Boats b1 WHERE R.H >= 102 and s.B=100 and s2.A = 6 and R.H < 104 and s.A = R.G and s2.A<s2.B and b1.D<103;




SELECT * FROM Sailors S,Reserves R WHERE S.A = R.G AND S.B<R.H;

SELECT * FROM Sailors S1,Sailors S2 WHERE S1.A<S2.A;
SELECT * FROM Sailors;
