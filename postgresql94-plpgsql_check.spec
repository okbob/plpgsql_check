%global pgmajorversion 94
%global pginstdir /usr/pgsql-9.4
%global sname plpgsql_check

Name:		%{sname}_%{pgmajorversion}
Version:	1.0.2
Release:	2%{?dist}
Summary:	Additional tools for plpgsql functions validation

Group:		Applications/Databases
License:	BSD
URL:		https://github.com/okbob/plpgsql_check/archive/v1.0.2.zip
Source0:	plpgsql_check-1.0.2.zip
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	postgresql%{pgmajorversion}-devel
Requires:	postgresql%{pgmajorversion}

%description
The plpgsql_check is PostgreSQL extension with functionality for direct
or indirect extra validation of functions in plpgsql language. It verifies
a validity of SQL identifiers used in plpgsql code. It try to identify
a performance issues.

%prep
%setup -q -n %{sname}-1.0.2


%build
PATH="%{pginstdir}/bin;$PATH" ; export PATH
CFLAGS="${CFLAGS:-%optflags}" ; export CFLAGS

USE_PGXS=1 make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make USE_PGXS=1 DESTDIR=%{buildroot} install

%clean
rm -rf %{buildroot}

%files
%defattr(644,root,root,755)
%doc README.md
%{pginstdir}/lib/plpgsql_check.so
%{pginstdir}/share/extension/plpgsql_check--1.0.sql
%{pginstdir}/share/extension/plpgsql_check.control


%changelog
* Thu Jul 09 2015 - Pavel STEHULE <pavel.stehule@gmail.com> 1.0.2-2
- bugfix release

* Fri Dec 19 2014 - Pavel STEHULE <pavel.stehule@gmail.com> 0.9.3-1
- fix a broken record field type checking
- add check for assign to array field

* Mon Aug 25 2014 - Pavel STEHULE <pavel.stehule@gmail.com> 0.9.1-1
- Initial packaging
